package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ActorRegistry implements kernel/actorreg.Registry over a channel-local
// sqlite. Each *ActorRegistry is bound to one channel database.
type ActorRegistry struct {
	db     *sql.DB
	fence  WriteFence
	outbox AppendObserver
}

// NewActorRegistry returns a registry over the given channel sqlite.
func NewActorRegistry(db *sql.DB) *ActorRegistry { return &ActorRegistry{db: db} }

// NewActorRegistryWithObservers wires optional same-transaction dependencies
// used by framework-owned member transition projections.
func NewActorRegistryWithObservers(db *sql.DB, fence WriteFence, outbox AppendObserver) *ActorRegistry {
	return &ActorRegistry{db: db, fence: fence, outbox: outbox}
}

// NewActorRegistryWithFence wires only the pure fencing gate.
func NewActorRegistryWithFence(db *sql.DB, fence WriteFence) *ActorRegistry {
	return NewActorRegistryWithObservers(db, fence, nil)
}

// Lookup implements actorreg.Registry.
func (r *ActorRegistry) Lookup(ctx context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                 COALESCE(display_name,''), created_at,
	                 COALESCE(deregistered_at,0),
	                 COALESCE(ready_state,'unknown'),
	                 COALESCE(ready_reason,'unknown'),
	                 COALESCE(ready_detail,'{}'),
	                 COALESCE(last_ready_at,0),
	                 COALESCE(last_state_change_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec actorreg.Record
	var kind, binding, readyState, readyReason, readyDetail string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt, &rec.DeregisteredAt,
		&readyState, &readyReason, &readyDetail, &rec.Readiness.LastReadyAt, &rec.Readiness.LastStateChangeAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return actorreg.Record{}, false, nil
	}
	if err != nil {
		return actorreg.Record{}, false, fmt.Errorf("store: actor lookup %q: %w", id, err)
	}
	rec.Kind = actor.Kind(kind)
	rec.Binding = actor.Binding(binding)
	rec.Readiness.State = actorreg.ReadinessState(readyState)
	rec.Readiness.Reason = readyReason
	rec.Readiness.Detail = json.RawMessage(readyDetail)
	rec.Readiness = rec.Readiness.Normalize()
	return rec, true, nil
}

// Exists implements actorreg.Registry — returns true even for soft-deregistered.
func (r *ActorRegistry) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
	const q = `SELECT 1 FROM actor_registry WHERE actor_id=? LIMIT 1`
	var one int
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: actor exists %q: %w", id, err)
	}
	return true, nil
}

// ListActive implements actorreg.Registry.
func (r *ActorRegistry) ListActive(ctx context.Context) ([]actorreg.Record, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                 COALESCE(display_name,''), created_at,
	                 COALESCE(ready_state,'unknown'),
	                 COALESCE(ready_reason,'unknown'),
	                 COALESCE(ready_detail,'{}'),
	                 COALESCE(last_ready_at,0),
	                 COALESCE(last_state_change_at,0)
	            FROM actor_registry
	            WHERE deregistered_at IS NULL
	            ORDER BY actor_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list active actors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []actorreg.Record
	for rows.Next() {
		var rec actorreg.Record
		var kind, binding, readyState, readyReason, readyDetail string
		if err := rows.Scan(&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt,
			&readyState, &readyReason, &readyDetail, &rec.Readiness.LastReadyAt, &rec.Readiness.LastStateChangeAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		rec.Kind = actor.Kind(kind)
		rec.Binding = actor.Binding(binding)
		rec.Readiness.State = actorreg.ReadinessState(readyState)
		rec.Readiness.Reason = readyReason
		rec.Readiness.Detail = json.RawMessage(readyDetail)
		rec.Readiness = rec.Readiness.Normalize()
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active actors rows: %w", err)
	}
	return out, nil
}

// Insert implements actorreg.Registry. Per L2 §1.4.6 invariant, the
// actor_cursors row is seeded in the same transaction.
func (r *ActorRegistry) Insert(ctx context.Context, rec actorreg.Record) error {
	if rec.ID == "" {
		return errors.New("store: actor insert: empty ID")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor insert begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	readiness := rec.Readiness.Normalize()
	if readiness.LastStateChangeAt == 0 {
		readiness.LastStateChangeAt = rec.CreatedAt
	}
	const insActor = `INSERT INTO actor_registry
	   (actor_id, actor_kind, actor_binding, display_name, created_at, deregistered_at,
	    ready_state, ready_reason, ready_detail, last_ready_at, last_state_change_at)
	   VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)`
	var binding any
	if rec.Binding == "" {
		binding = nil
	} else {
		binding = string(rec.Binding)
	}
	var displayName any
	if rec.DisplayName == "" {
		displayName = nil
	} else {
		displayName = rec.DisplayName
	}
	if _, err := tx.ExecContext(ctx, insActor,
		string(rec.ID), string(rec.Kind), binding, displayName, rec.CreatedAt,
		string(readiness.State), readiness.Reason, string(readiness.Detail),
		readiness.LastReadyAt, readiness.LastStateChangeAt,
	); err != nil {
		return fmt.Errorf("store: actor insert %q: %w", rec.ID, err)
	}

	const insCursor = `INSERT OR IGNORE INTO actor_cursors
	   (actor_id, last_consumed_seq, last_consumed_id, updated_at)
	   VALUES (?, 0, NULL, ?)`
	if _, err := tx.ExecContext(ctx, insCursor, string(rec.ID), rec.CreatedAt); err != nil {
		return fmt.Errorf("store: cursor seed %q: %w", rec.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor insert commit: %w", err)
	}
	return nil
}

// Deregister implements actorreg.Registry.
func (r *ActorRegistry) Deregister(ctx context.Context, id actor.ActorID, at int64) error {
	const q = `UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`
	res, err := r.db.ExecContext(ctx, q, at, string(id))
	if err != nil {
		return fmt.Errorf("store: actor deregister %q: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either missing or already deregistered — caller treats as no-op.
		return nil
	}
	return nil
}

// UpdateReadiness writes the actor_registry readiness projection and
// reports whether the externally visible state/reason changed.
func (r *ActorRegistry) UpdateReadiness(ctx context.Context, id actor.ActorID, update actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
	if id == "" {
		return actorreg.ReadinessTransition{}, errors.New("store: actor readiness update: empty ID")
	}
	next := actorreg.Readiness{
		State:  update.State,
		Reason: update.Reason,
		Detail: append(json.RawMessage(nil), update.Detail...),
	}.Normalize()
	switch next.State {
	case actorreg.ReadinessReady, actorreg.ReadinessNotReady, actorreg.ReadinessUnknown:
	default:
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update %q invalid state %q", id, next.State)
	}
	if !json.Valid(next.Detail) {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update %q invalid detail JSON", id)
	}
	checkedAt := update.CheckedAt

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var prev actorreg.Readiness
	var prevState, prevReason, prevDetail string
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(ready_state,'unknown'),
		       COALESCE(ready_reason,'unknown'),
		       COALESCE(ready_detail,'{}'),
		       COALESCE(last_ready_at,0),
		       COALESCE(last_state_change_at,0)
		  FROM actor_registry
		 WHERE actor_id=?`, string(id)).
		Scan(&prevState, &prevReason, &prevDetail, &prev.LastReadyAt, &prev.LastStateChangeAt)
	if errors.Is(err, sql.ErrNoRows) {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update %q: actor not found", id)
	}
	if err != nil {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update lookup %q: %w", id, err)
	}
	prev.State = actorreg.ReadinessState(prevState)
	prev.Reason = prevReason
	prev.Detail = json.RawMessage(prevDetail)
	prev = prev.Normalize()

	stateReasonChanged := prev.State != next.State || prev.Reason != next.Reason
	lastReadyAt := prev.LastReadyAt
	if next.State == actorreg.ReadinessReady {
		lastReadyAt = checkedAt
	}
	lastStateChangeAt := prev.LastStateChangeAt
	if stateReasonChanged {
		lastStateChangeAt = checkedAt
	}
	next.LastReadyAt = lastReadyAt
	next.LastStateChangeAt = lastStateChangeAt
	changed := prev.State != next.State ||
		prev.Reason != next.Reason ||
		string(prev.Detail) != string(next.Detail) ||
		prev.LastReadyAt != next.LastReadyAt ||
		prev.LastStateChangeAt != next.LastStateChangeAt

	if _, err := tx.ExecContext(ctx, `
		UPDATE actor_registry
		   SET ready_state=?,
		       ready_reason=?,
		       ready_detail=?,
		       last_ready_at=?,
		       last_state_change_at=?
		 WHERE actor_id=?`,
		string(next.State), next.Reason, string(next.Detail),
		next.LastReadyAt, next.LastStateChangeAt, string(id)); err != nil {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update %q: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return actorreg.ReadinessTransition{}, fmt.Errorf("store: actor readiness update commit: %w", err)
	}
	return actorreg.ReadinessTransition{Previous: prev, Current: next, Changed: changed}, nil
}

// MemberActorAdd is one daemon-side actor registration transition derived
// from catalog channel_members.
type MemberActorAdd struct {
	ID          actor.ActorID
	Kind        actor.Kind
	Binding     actor.Binding
	DisplayName string
	UserID      string
	Role        string
	At          int64
	ProxyHost   MemberActorProxyHost

	// CapabilitySet is the opaque declaration blob that lets a reconciler
	// rebuild the actor's facade wiring from the channel log alone
	// (推论5 / §4 事实完整性). For runtime_inbound_via_relay proxy actors it
	// carries the proxy_facade.CapabilitySet (name / types / type
	// declarations / max_pending_ms / binding hints) so that
	// proxyfacade.DeclarationFromCapability can reconstruct the facade
	// declaration without any live update_members frame. Empty for
	// members that need no facade wiring (humans / channel-agent). It is
	// echoed verbatim into the system.actor.registered fact and is never
	// interpreted by the store.
	CapabilitySet json.RawMessage
}

// MemberActorProxyHost is optional metadata for proxy-daemon-hosted actors.
// It is emitted in system.actor.registered only; actor_registry remains the
// daemon-local framework registry, not a host metadata store.
type MemberActorProxyHost struct {
	DaemonID   string
	DaemonName string
}

// MemberActorRemove is one daemon-side actor deregistration transition.
type MemberActorRemove struct {
	ID actor.ActorID
	At int64
}

// ApplyMemberTransitions mutates actor_registry and appends the matching
// system.actor.* mirror events in one sqlite transaction. Duplicate retries
// are idempotent: already-active adds and already-deregistered removes do not
// append a second event.
func (r *ActorRegistry) ApplyMemberTransitions(
	ctx context.Context,
	channelID channel.ID,
	adds []MemberActorAdd,
	removes []MemberActorRemove,
	fencing klog.FencingTuple,
) error {
	if channelID == "" {
		return errors.New("store: actor member transition: empty channel_id")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor member transition begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	msgs := NewMessagesWithObservers(r.db, r.fence, r.outbox)
	for _, add := range adds {
		if add.ID == "" {
			continue
		}
		if add.Kind == "" {
			add.Kind = actor.KindHuman
		}
		if add.At == 0 {
			return fmt.Errorf("store: actor member add %q missing timestamp", add.ID)
		}
		changed, err := r.applyMemberAddTx(ctx, tx, add)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		env, err := actorRegisteredEnvelope(channelID, add)
		if err != nil {
			return err
		}
		if _, err := msgs.AppendTx(ctx, tx, env, fencing); err != nil {
			return fmt.Errorf("store: actor registered mirror %q: %w", add.ID, err)
		}
	}
	for _, remove := range removes {
		if remove.ID == "" {
			continue
		}
		if remove.At == 0 {
			return fmt.Errorf("store: actor member remove %q missing timestamp", remove.ID)
		}
		changed, err := r.applyMemberRemoveTx(ctx, tx, remove)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		env, err := actorDeregisteredEnvelope(channelID, remove)
		if err != nil {
			return err
		}
		if _, err := msgs.AppendTx(ctx, tx, env, fencing); err != nil {
			return fmt.Errorf("store: actor deregistered mirror %q: %w", remove.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor member transition commit: %w", err)
	}
	return nil
}

// DesiredProxyMember is one runtime_inbound_via_relay tool actor derived
// purely by replaying the channel log's system.actor.registered /
// system.actor.deregistered facts (推论5 / §4 事实完整性). It is the
// projection-of-record the per-channel Reconciler consumes to rebuild proxy
// facade wiring without any live update_members frame — exactly the input
// required by §9 DoD #4 ("仅靠 facts 能重建出全部 facade/handler").
type DesiredProxyMember struct {
	ID            actor.ActorID
	CapabilitySet json.RawMessage
}

// ListDesiredProxyMembers replays the append-only log's actor registration
// facts and returns the current set of active runtime_inbound_via_relay tool
// actors together with the capability_set blob carried on their latest
// registered fact. Replay semantics:
//
//   - facts are read in (seq) order so a later deregistered fact removes an
//     earlier registered one, and a re-registration after a deregister
//     re-adds the actor (level-triggered: the final fact wins).
//   - only kind=tool + binding=runtime_inbound_via_relay registered facts are
//     considered desired proxy facades; everything else (humans, system,
//     channel-agent, static-factory adapters) is wired through other paths.
//
// The result is the authoritative desired set: it never reads the
// actor_registry projection nor any live wiring, so a Reconciler that
// rebuilds from this alone is replay-correct after a daemon restart or a
// cleared in-process wiring table.
func (r *ActorRegistry) ListDesiredProxyMembers(ctx context.Context) ([]DesiredProxyMember, error) {
	const q = `SELECT type, payload
	             FROM messages
	            WHERE type IN ('system.actor.registered','system.actor.deregistered')
	            ORDER BY seq ASC`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list desired proxy members: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type regPayload struct {
		ActorID       actor.ActorID   `json:"actor_id"`
		Kind          actor.Kind      `json:"actor_kind"`
		Binding       actor.Binding   `json:"actor_binding"`
		CapabilitySet json.RawMessage `json:"capability_set"`
	}
	type deregPayload struct {
		ActorID actor.ActorID `json:"actor_id"`
	}
	// Preserve first-registration order for deterministic wiring; the map
	// tracks the live capability_set for re-registrations.
	order := make([]actor.ActorID, 0, 8)
	current := make(map[actor.ActorID]json.RawMessage)
	for rows.Next() {
		var typ, payload string
		if err := rows.Scan(&typ, &payload); err != nil {
			return nil, fmt.Errorf("store: list desired proxy members scan: %w", err)
		}
		switch typ {
		case "system.actor.registered":
			var p regPayload
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return nil, fmt.Errorf("store: decode registered fact: %w", err)
			}
			if p.Kind != actor.KindTool || p.Binding != actor.BindingRuntimeInboundViaRelay {
				continue
			}
			if _, seen := current[p.ActorID]; !seen {
				order = append(order, p.ActorID)
			}
			current[p.ActorID] = append(json.RawMessage(nil), p.CapabilitySet...)
		case "system.actor.deregistered":
			var p deregPayload
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return nil, fmt.Errorf("store: decode deregistered fact: %w", err)
			}
			delete(current, p.ActorID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list desired proxy members rows: %w", err)
	}
	out := make([]DesiredProxyMember, 0, len(current))
	for _, id := range order {
		cap, ok := current[id]
		if !ok {
			continue
		}
		out = append(out, DesiredProxyMember{ID: id, CapabilitySet: cap})
	}
	return out, nil
}

func (r *ActorRegistry) applyMemberAddTx(ctx context.Context, tx *sql.Tx, add MemberActorAdd) (bool, error) {
	var deregistered sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT deregistered_at FROM actor_registry WHERE actor_id=?`, string(add.ID)).Scan(&deregistered)
	switch {
	case err == nil:
		if !deregistered.Valid {
			// Row is already active (reconnect / retried update_members /
			// re-run of a boot-time seed). A duplicate add is normally an
			// idempotent no-op, BUT only when the incoming fact matches the
			// one already on the log. If a retry carries a DIFFERENT
			// capability_set than the active registered fact, silently
			// no-op'ing would freeze a stale/incomplete fact in place with no
			// way to repair it (the reconciler rebuilds from the latest fact,
			// not from this frame). Reject so the caller must remove+add to
			// change capability. Identical capability stays a no-op (suppresses
			// a duplicate system.actor.registered fact).
			existing, err := latestRegisteredCapabilityTx(ctx, tx, add.ID)
			if err != nil {
				return false, err
			}
			if !sameCapabilityFact(existing, add.CapabilitySet) {
				return false, fmt.Errorf(
					"store: actor %q active duplicate add with conflicting capability_set; remove+add required to change capability",
					add.ID)
			}
			return false, nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE actor_registry
			    SET actor_kind=?, actor_binding=?, display_name=?, created_at=?, deregistered_at=NULL,
			        ready_state='unknown', ready_reason='unknown', ready_detail='{}',
			        last_ready_at=0, last_state_change_at=?
			  WHERE actor_id=?`,
			string(add.Kind), nullableString(string(add.Binding)), nullableString(add.DisplayName), add.At, add.At, string(add.ID),
		)
		if err != nil {
			return false, fmt.Errorf("store: actor reactivate %q: %w", add.ID, err)
		}
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		_, err := tx.ExecContext(ctx,
			`INSERT INTO actor_registry
			   (actor_id, actor_kind, actor_binding, display_name, created_at, deregistered_at,
			    ready_state, ready_reason, ready_detail, last_ready_at, last_state_change_at)
			 VALUES (?, ?, ?, ?, ?, NULL, 'unknown', 'unknown', '{}', 0, ?)`,
			string(add.ID), string(add.Kind), nullableString(string(add.Binding)), nullableString(add.DisplayName), add.At, add.At,
		)
		if err != nil {
			return false, fmt.Errorf("store: actor member insert %q: %w", add.ID, err)
		}
		_, err = tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO actor_cursors
			   (actor_id, last_consumed_seq, last_consumed_id, updated_at)
			 VALUES (?, 0, NULL, ?)`,
			string(add.ID), add.At,
		)
		if err != nil {
			return false, fmt.Errorf("store: actor member cursor seed %q: %w", add.ID, err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("store: actor member lookup %q: %w", add.ID, err)
	}
}

func (r *ActorRegistry) applyMemberRemoveTx(ctx context.Context, tx *sql.Tx, remove MemberActorRemove) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`,
		remove.At, string(remove.ID),
	)
	if err != nil {
		return false, fmt.Errorf("store: actor member deregister %q: %w", remove.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// latestRegisteredCapabilityTx returns the capability_set blob carried on the
// actor's most recent system.actor.registered fact (the projection-of-record
// the reconciler rebuilds from). Returns nil when the actor has no registered
// fact or the latest fact carried no capability_set. Read inside the same tx so
// the duplicate-add consistency check sees a consistent snapshot.
func latestRegisteredCapabilityTx(ctx context.Context, tx *sql.Tx, id actor.ActorID) (json.RawMessage, error) {
	const q = `SELECT payload
	             FROM messages
	            WHERE type='system.actor.registered'
	            ORDER BY seq DESC`
	rows, err := tx.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: latest registered capability %q: %w", id, err)
	}
	defer func() { _ = rows.Close() }()
	type regPayload struct {
		ActorID       actor.ActorID   `json:"actor_id"`
		CapabilitySet json.RawMessage `json:"capability_set"`
	}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("store: latest registered capability scan %q: %w", id, err)
		}
		var p regPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, fmt.Errorf("store: decode registered fact for %q: %w", id, err)
		}
		if p.ActorID == id {
			return p.CapabilitySet, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: latest registered capability rows %q: %w", id, err)
	}
	return nil, nil
}

// sameCapabilityFact reports whether two capability_set blobs are the same fact
// for duplicate-add idempotency purposes. Both empty / null count as equal; a
// non-empty blob is compared by canonical JSON (key-order-independent) so a
// re-marshalled-but-semantically-identical retry stays a no-op.
func sameCapabilityFact(a, b json.RawMessage) bool {
	an := isEmptyCapability(a)
	bn := isEmptyCapability(b)
	if an || bn {
		return an && bn
	}
	ca, errA := canonicalJSON(a)
	cb, errB := canonicalJSON(b)
	if errA != nil || errB != nil {
		// Unparsable blob: fall back to raw byte equality rather than
		// declaring a false match.
		return bytes.Equal(a, b)
	}
	return bytes.Equal(ca, cb)
}

func isEmptyCapability(b json.RawMessage) bool {
	t := bytes.TrimSpace(b)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

func canonicalJSON(b json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func actorRegisteredEnvelope(channelID channel.ID, add MemberActorAdd) (*message.Envelope, error) {
	payloadMap := map[string]any{
		"actor_id":      add.ID,
		"actor_kind":    add.Kind,
		"actor_binding": add.Binding,
		"display_name":  add.DisplayName,
		"user_id":       add.UserID,
		"role":          add.Role,
		"registered_at": add.At,
	}
	if add.ProxyHost.DaemonID != "" || add.ProxyHost.DaemonName != "" {
		payloadMap["proxy_host"] = map[string]any{
			"daemon_id":   add.ProxyHost.DaemonID,
			"daemon_name": add.ProxyHost.DaemonName,
		}
	}
	// 推论5 / §4 事实完整性: carry the facade-rebuild declaration blob into
	// the fact so a reconciler can replay the channel log and reconstruct
	// proxy facade wiring without any live update_members frame. The store
	// treats it as opaque; only validity (well-formed JSON object) is
	// checked so the canonical hash stays deterministic.
	if len(add.CapabilitySet) > 0 && !bytes.Equal(bytes.TrimSpace(add.CapabilitySet), []byte("null")) {
		payloadMap["capability_set"] = json.RawMessage(add.CapabilitySet)
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return nil, err
	}
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("system.actor.registered:%s:%d", add.ID, add.At)),
		TS:         add.At,
		TSReceived: add.At,
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.actor.registered",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
	hash, err := message.CanonicalHash(*env)
	env.CanonicalHash = hash
	return env, err
}

func actorDeregisteredEnvelope(channelID channel.ID, remove MemberActorRemove) (*message.Envelope, error) {
	payload, err := json.Marshal(map[string]any{
		"actor_id":        remove.ID,
		"deregistered_at": remove.At,
	})
	if err != nil {
		return nil, err
	}
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("system.actor.deregistered:%s:%d", remove.ID, remove.At)),
		TS:         remove.At,
		TSReceived: remove.At,
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.actor.deregistered",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
	hash, err := message.CanonicalHash(*env)
	env.CanonicalHash = hash
	return env, err
}
