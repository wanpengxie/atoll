package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ActorRegistry implements kernel/actorreg.Registry over a channel-local
// sqlite. Each *ActorRegistry is bound to one channel database.
type ActorRegistry struct {
	db *sql.DB
}

// NewActorRegistry returns a registry over the given channel sqlite.
func NewActorRegistry(db *sql.DB) *ActorRegistry { return &ActorRegistry{db: db} }

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

	msgs := NewMessagesWithLock(r.db, NewChannelLock(r.db))
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

// BackfillRegisteredFacts appends a system.actor.registered fact for every
// active actor_registry row that does not already have one in the channel
// log, making membership a projection-of-record replayable from the log
// alone (推论5 / §4 事实完整性). It exists because actors can be born
// through paths that write actor_registry directly without an envelope:
//   - bootstrap saga seeds (system actor / initial members / adapter seeds),
//   - ensureChannelAgent (the per-channel agent target),
//   - channels created before the fact carried full declaration data.
//
// The method is idempotent and replay-safe: it skips any actor that already
// has a system.actor.registered:<id>:* fact and re-uses the row's created_at
// as the deterministic event timestamp so a re-run produces the same id. It
// does NOT carry capability_set — only the proxy-facade update_members path
// (ApplyMemberTransitions) has that blob; backfilled rows are humans /
// system / channel-agent / static-factory adapters whose facade (if any) is
// rebuilt from compiled-in module factories, not from log data.
//
// Soft-deregistered rows are skipped: their lifecycle already produced (or
// will produce) the registered+deregistered fact pair through the normal
// transition path, and re-asserting a registered fact for a dead actor would
// lie about current membership.
func (r *ActorRegistry) BackfillRegisteredFacts(
	ctx context.Context,
	channelID channel.ID,
	fencing klog.FencingTuple,
) error {
	if channelID == "" {
		return errors.New("store: backfill registered facts: empty channel_id")
	}
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                  COALESCE(display_name,''), created_at
	             FROM actor_registry
	            WHERE deregistered_at IS NULL
	            ORDER BY created_at ASC, actor_id ASC`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return fmt.Errorf("store: backfill registered facts list: %w", err)
	}
	type rowT struct {
		id          actor.ActorID
		kind        actor.Kind
		binding     actor.Binding
		displayName string
		createdAt   int64
	}
	var pending []rowT
	for rows.Next() {
		var rt rowT
		var kind, binding, displayName string
		if err := rows.Scan(&rt.id, &kind, &binding, &displayName, &rt.createdAt); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: backfill registered facts scan: %w", err)
		}
		rt.kind = actor.Kind(kind)
		rt.binding = actor.Binding(binding)
		rt.displayName = displayName
		pending = append(pending, rt)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("store: backfill registered facts rows: %w", err)
	}
	_ = rows.Close()
	if len(pending) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: backfill registered facts begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	msgs := NewMessagesWithLock(r.db, NewChannelLock(r.db))
	appended := 0
	for _, rt := range pending {
		// A registered fact already on the log (under any timestamp)
		// means this actor is already projection-of-record complete.
		var present int
		err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM messages
			  WHERE type='system.actor.registered'
			    AND id LIKE ? ESCAPE '\'
			  LIMIT 1`,
			"system.actor.registered:"+escapeLike(string(rt.id))+":%").Scan(&present)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("store: backfill registered facts presence %q: %w", rt.id, err)
		}
		at := rt.createdAt
		if at == 0 {
			at = 1
		}
		env, err := actorRegisteredEnvelope(channelID, MemberActorAdd{
			ID:          rt.id,
			Kind:        rt.kind,
			Binding:     rt.binding,
			DisplayName: rt.displayName,
			At:          at,
		})
		if err != nil {
			return err
		}
		if _, err := msgs.AppendTx(ctx, tx, env, fencing); err != nil {
			return fmt.Errorf("store: backfill registered fact %q: %w", rt.id, err)
		}
		appended++
	}
	if appended == 0 {
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: backfill registered facts commit: %w", err)
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

// escapeLike escapes the SQLite LIKE wildcards in an actor id so a
// presence probe matches the literal id rather than treating embedded
// %/_ as patterns. The probe uses ESCAPE '\'.
func escapeLike(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(s)
}

func (r *ActorRegistry) applyMemberAddTx(ctx context.Context, tx *sql.Tx, add MemberActorAdd) (bool, error) {
	var deregistered sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT deregistered_at FROM actor_registry WHERE actor_id=?`, string(add.ID)).Scan(&deregistered)
	switch {
	case err == nil:
		if !deregistered.Valid {
			// Row is already active (reconnect / retried update_members).
			// Normally a duplicate add is a no-op, but the channel log is the
			// only durable source of capability_set: a proxy that reconnects
			// carrying a (possibly newly-complete) capability_set must be able
			// to REPAIR the log even though the registry row already exists
			// (推论5 / §4 事实完整性; codex P1 actors.go:340). Re-emit the
			// registered fact only when the incoming capability differs from
			// the latest active fact, so retries stay idempotent and the later
			// complete fact wins on replay (ListDesiredProxyMembers, seq ASC).
			if r.shouldRepairProxyFact(ctx, tx, add) {
				return true, nil
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

// shouldRepairProxyFact reports whether an add for an already-active proxy
// (kind=tool, binding=runtime_inbound_via_relay) actor must re-emit a
// system.actor.registered fact to repair the log. It returns true when the
// incoming add carries a capability_set that differs from the capability_set
// on the actor's latest active registered fact (or when no usable capability
// is on the log yet but the add now carries one). Non-proxy adds, and proxy
// adds whose capability already matches the latest fact, return false so
// retried/duplicate update_members frames stay idempotent.
//
// Replay note: ListDesiredProxyMembers reads facts in seq ASC and lets the
// last registered fact win, so a re-emitted fact at a higher seq supersedes a
// legacy fact that lacked capability_set (the design's "later complete fact
// wins").
func (r *ActorRegistry) shouldRepairProxyFact(ctx context.Context, tx *sql.Tx, add MemberActorAdd) bool {
	if add.Kind != actor.KindTool || add.Binding != actor.BindingRuntimeInboundViaRelay {
		return false
	}
	incoming := normalizedCapability(add.CapabilitySet)
	if len(incoming) == 0 {
		// Nothing to repair with — never re-emit an empty capability.
		return false
	}
	latest, ok := r.latestActiveProxyCapability(ctx, tx, add.ID)
	if !ok {
		// No registered fact on the log yet for an active proxy row: the add
		// carries capability, so emit to make the log projection-of-record.
		return true
	}
	return !bytes.Equal(normalizedCapability(latest), incoming)
}

// latestActiveProxyCapability returns the capability_set on the actor's most
// recent registered fact, or ok=false when its lifecycle's latest terminal
// fact is a deregister (or no registered fact exists). It replays only this
// actor's facts (seq ASC, last wins) so it observes the same effective state
// ListDesiredProxyMembers does.
func (r *ActorRegistry) latestActiveProxyCapability(ctx context.Context, tx *sql.Tx, id actor.ActorID) (json.RawMessage, bool) {
	const q = `SELECT type, payload
	             FROM messages
	            WHERE type IN ('system.actor.registered','system.actor.deregistered')
	              AND id LIKE ? ESCAPE '\'
	            ORDER BY seq ASC`
	rows, err := tx.QueryContext(ctx, q, "%:"+escapeLike(string(id))+":%")
	if err != nil {
		return nil, false
	}
	defer func() { _ = rows.Close() }()
	type regPayload struct {
		ActorID       actor.ActorID   `json:"actor_id"`
		CapabilitySet json.RawMessage `json:"capability_set"`
	}
	var cap json.RawMessage
	active := false
	for rows.Next() {
		var typ, payload string
		if err := rows.Scan(&typ, &payload); err != nil {
			return nil, false
		}
		switch typ {
		case "system.actor.registered":
			var p regPayload
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return nil, false
			}
			if p.ActorID != id {
				continue
			}
			cap = append(json.RawMessage(nil), p.CapabilitySet...)
			active = true
		case "system.actor.deregistered":
			var p struct {
				ActorID actor.ActorID `json:"actor_id"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return nil, false
			}
			if p.ActorID != id {
				continue
			}
			cap = nil
			active = false
		}
	}
	if rows.Err() != nil {
		return nil, false
	}
	return cap, active
}

// normalizedCapability strips an absent/null capability_set down to a nil
// slice so presence comparisons treat "missing" and "null" identically.
func normalizedCapability(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	return raw
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
