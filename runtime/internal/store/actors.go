package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorRegistry implements runtime/storespec.Registry over a channel-local
// sqlite. Each *actorRegistry is bound to one channel database AND its channel
// id — that bound id is the scope its mirror events carry, never a per-call arg.
type actorRegistry struct {
	db        *sql.DB
	channelID channel.ID
	// onCommit is the same substrate commit signal source messages holds: the
	// control-plane mirror append commits truth too, so this path fires the
	// signal as well — both write paths are one chokepoint to the tap. nil = no
	// subscriber wired. See messages.onCommit.
	onCommit func()
}

// NewActorRegistry returns a registry over the given channel sqlite, bound to
// channelID — the scope its membership mirror events are stamped with.
// (v2: no fence / outbox — single write path by construction; the store is a
// pure persistence sink, so same-tx side projections do not live here.)
func newActorRegistry(db *sql.DB, channelID channel.ID, onCommit func()) *actorRegistry {
	return &actorRegistry{db: db, channelID: channelID, onCommit: onCommit}
}

// Lookup implements storespec.Registry.
func (r *actorRegistry) Lookup(ctx context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''), COALESCE(host,''),
	                 created_at, COALESCE(deregistered_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec storespec.Record
	var kind, binding string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &binding, &rec.Host, &rec.CreatedAt, &rec.DeregisteredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.Record{}, false, nil
	}
	if err != nil {
		return storespec.Record{}, false, fmt.Errorf("store: actor lookup %q: %w", id, err)
	}
	k, ok := actor.ParseKind(kind)
	if !ok {
		return storespec.Record{}, false, fmt.Errorf("store: actor %q invalid kind %q (out of closed set)", id, kind)
	}
	rec.Kind = k
	// Binding read symmetric with kind: validate against the closed-set contract
	// (the public predicate ParseBinding, per binding.go) instead of a raw cast.
	// Empty is a legitimate state (a cell-less member — e.g. a human — has no
	// binding), so only a non-empty value is parsed; a non-empty out-of-set value
	// is a poisoned row and fails loudly, exactly as a poison kind does.
	if binding != "" {
		b, ok := actor.ParseBinding(binding)
		if !ok {
			return storespec.Record{}, false, fmt.Errorf("store: actor %q invalid binding %q (out of closed set)", id, binding)
		}
		rec.Binding = b
	}
	return rec, true, nil
}

// Exists implements storespec.Registry — returns true even for soft-deregistered.
func (r *actorRegistry) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
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

// ListActive implements storespec.Registry.
func (r *actorRegistry) ListActive(ctx context.Context) ([]storespec.Record, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''), COALESCE(host,''),
	                 created_at
	            FROM actor_registry
	            WHERE deregistered_at IS NULL
	            ORDER BY actor_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list active actors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []storespec.Record
	for rows.Next() {
		var rec storespec.Record
		var kind, binding string
		if err := rows.Scan(&rec.ID, &kind, &binding, &rec.Host, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		k, ok := actor.ParseKind(kind)
		if !ok {
			return nil, fmt.Errorf("store: list active actors invalid kind %q (out of closed set)", kind)
		}
		rec.Kind = k
		if binding != "" {
			b, ok := actor.ParseBinding(binding)
			if !ok {
				return nil, fmt.Errorf("store: list active actors invalid binding %q (out of closed set)", binding)
			}
			rec.Binding = b
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active actors rows: %w", err)
	}
	return out, nil
}

// validateMemberIdentity gates the membership WRITE path on the protocol
// closed sets — the control-plane twin of the envelope write path's
// stepSenderConsistent ParseKind gate, and for the same reason: the read path
// (Lookup / ListActive) parses these columns fail-loud, so an unvalidated
// write is a poisoned row that only explodes on a later read — and because
// ListActive fails on the FIRST bad row, one poison row bricks the whole
// channel's member enumeration (reconcile, door membership checks). Binding
// is nullable: empty means NULL, any non-empty value must parse.
func validateMemberIdentity(id actor.ActorID, kind actor.Kind, binding actor.Binding) error {
	if _, ok := actor.ParseKind(string(kind)); !ok {
		return fmt.Errorf("store: actor %q kind %q not in the actor.Kind closed set", id, kind)
	}
	if binding != "" {
		if _, ok := actor.ParseBinding(string(binding)); !ok {
			return fmt.Errorf("store: actor %q binding %q not in the actor.Binding closed set", id, binding)
		}
	}
	return nil
}

// Insert implements storespec.Registry: it adds one membership row.
func (r *actorRegistry) Insert(ctx context.Context, rec storespec.Record) error {
	if rec.ID == "" {
		return errors.New("store: actor insert: empty ID")
	}
	if err := validateMemberIdentity(rec.ID, rec.Kind, rec.Binding); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor insert begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insActor = `INSERT INTO actor_registry
	   (actor_id, actor_kind, actor_binding, host, created_at, deregistered_at)
	   VALUES (?, ?, ?, ?, ?, NULL)`
	var binding any
	if rec.Binding == "" {
		binding = nil
	} else {
		binding = string(rec.Binding)
	}
	if _, err := tx.ExecContext(ctx, insActor,
		string(rec.ID), string(rec.Kind), binding, rec.Host, rec.CreatedAt,
	); err != nil {
		return fmt.Errorf("store: actor insert %q: %w", rec.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor insert commit: %w", err)
	}
	return nil
}

// Deregister implements storespec.Registry. It runs in a transaction so the
// deregistration transition and the actor-scoped state cascade (an owner's
// death implies its private state dies too) commit atomically. The no-op
// semantics are preserved: a missing or already-deregistered actor changes zero
// rows, so nothing is cascaded and no error is returned.
func (r *actorRegistry) Deregister(ctx context.Context, id actor.ActorID, at int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor deregister begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const q = `UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`
	res, err := tx.ExecContext(ctx, q, at, string(id))
	if err != nil {
		return fmt.Errorf("store: actor deregister %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		// Do NOT swallow: with n unknowable, falling into the no-op branch would
		// roll back an UPDATE that may have succeeded — "return nil = nothing
		// happened" must describe reality, never manufacture it.
		return fmt.Errorf("store: actor deregister rows-affected %q: %w", id, err)
	}
	if n == 0 {
		// Either missing or already deregistered — caller treats as no-op. Nothing
		// transitioned, so no cascade (idempotent; the rollback discards the empty
		// UPDATE).
		return nil
	}
	// The row transitioned to deregistered in THIS tx — cascade-clear its
	// actor-scoped state in the same tx (scope law, atomic with the transition).
	if err := clearActorScopedTx(ctx, tx, id); err != nil {
		return err
	}
	// Same tx: cascade-clear its identity-level pending timers. A parallel call,
	// not folded into the state cascade above — one locus, one function (see
	// clearTimersTx doc in timers.go).
	if err := clearTimersTx(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor deregister commit: %w", err)
	}
	return nil
}

// (clearActorScopedTx — the actor-scoped cascade both dereg entry points call —
// lives in state.go beside the locus's other SQL, so actor_state has exactly one
// author file. clearTimersTx is its parallel sibling for the timers locus,
// living in timers.go.)

// Membership transition DTOs (storespec.MemberActorAdd / storespec.MemberActorRemove)
// + the MembershipControlPlane contract live in runtime/storespec (contract
// types). This file is their sqlite implementation.

// ApplyMemberTransitions mutates actor_registry and appends the matching
// system.actor.* mirror events in one sqlite transaction. Duplicate retries
// are idempotent: already-active adds and already-deregistered removes do not
// append a second event.
//
// No per-call channelID: the scope is the binding (r.channelID, fixed at
// OpenChannel). A per-call channel arg was a pseudo-parameter the caller could
// lie about — stamping a foreign-channel mirror row into this channel's sqlite.
// The mirror event's channel is whatever this store IS, by construction (same
// nullification of illegal state as FindByID).
func (r *actorRegistry) ApplyMemberTransitions(
	ctx context.Context,
	adds []storespec.MemberActorAdd,
	removes []storespec.MemberActorRemove,
) error {
	if r.channelID == "" {
		return errors.New("store: actor member transition: registry not bound to a channel")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor member transition begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// appended counts mirror rows that actually landed — the post-commit signal
	// fires only when truth advanced (no-op transitions stay silent).
	appended := 0

	// v2: no fencing — the harness is the single REQUEST-PATH writer. These
	// system.actor.* mirror events are written with is_terminal=false (events
	// are never terminal).
	//
	// CONTROL-PLANE WRITE (structurally confined, not a convention exception):
	// membership is a control-plane mutation (cf. Slack admin API / Unix mount),
	// and its mirror event MUST be atomic with the actor_registry row it records
	// — they share this one tx, which the harness chain (a separate write path)
	// cannot join. This direct write is safe to expose ONLY because the whole
	// store sits under runtime/internal (see doc.go): business code physically
	// cannot reach it, so "bypass the harness" is not an ambient capability —
	// it is reachable solely through this named, runtime-internal control-plane
	// op. A new bypass write means a new named op here, never an ad-hoc Append.
	for _, add := range adds {
		if add.ID == "" {
			continue
		}
		// No silent kind default (the former ""→KindHuman fallback let a
		// kind-less daemon/tool add enter the registry AND the mirror truth
		// as a human, with zero signal): a missing kind is a caller bug and
		// fails loud, same posture as the missing-timestamp check below.
		if err := validateMemberIdentity(add.ID, add.Kind, add.Binding); err != nil {
			return err
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
		env := actorRegisteredEnvelope(r.channelID, add)
		if _, err := appendTx(ctx, tx, env, false); err != nil {
			return fmt.Errorf("store: actor registered mirror %q: %w", add.ID, err)
		}
		appended++
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
		env := actorDeregisteredEnvelope(r.channelID, remove)
		if _, err := appendTx(ctx, tx, env, false); err != nil {
			return fmt.Errorf("store: actor deregistered mirror %q: %w", remove.ID, err)
		}
		appended++
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor member transition commit: %w", err)
	}
	// A mirror row landed durably — fire the substrate commit signal so the tap
	// sees member events as promptly as request-path commits (no second write
	// path that silently bypasses the signal). No-op transitions append nothing,
	// so a spurious wake is avoided.
	if appended > 0 && r.onCommit != nil {
		r.onCommit()
	}
	return nil
}

func (r *actorRegistry) applyMemberAddTx(ctx context.Context, tx *sql.Tx, add storespec.MemberActorAdd) (bool, error) {
	var deregistered sql.NullInt64
	var curHost string
	err := tx.QueryRowContext(ctx,
		`SELECT deregistered_at, COALESCE(host,'') FROM actor_registry WHERE actor_id=?`,
		string(add.ID)).Scan(&deregistered, &curHost)
	switch {
	case err == nil:
		if !deregistered.Valid {
			// Row is already active (reconnect / retried update_members /
			// re-run of a boot-time seed). Substrate identity is {ID, Kind,
			// Binding} and carries no per-actor declaration to diff — that is an
			// application-level event (skill-as-document, runtime), not membership.
			// So a duplicate add is an idempotent no-op EXCEPT for a placement
			// move: host is a durable membership fact, and a compute re-homing an
			// actor (or the home reclaiming it) must be recorded. This is a
			// deliberate narrowing of the former unconditional no-op: a host-only
			// change UPDATEs the row but returns changed=false, so it emits NO
			// system.actor.registered mirror — the mirror never carries host, and
			// re-homing is not a re-registration.
			if curHost == add.Host {
				return false, nil
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE actor_registry SET host=? WHERE actor_id=?`,
				add.Host, string(add.ID),
			); err != nil {
				return false, fmt.Errorf("store: actor rehost %q: %w", add.ID, err)
			}
			return false, nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE actor_registry
			    SET actor_kind=?, actor_binding=?, host=?, created_at=?, deregistered_at=NULL
			  WHERE actor_id=?`,
			string(add.Kind), nullableString(string(add.Binding)), add.Host, add.At, string(add.ID),
		)
		if err != nil {
			return false, fmt.Errorf("store: actor reactivate %q: %w", add.ID, err)
		}
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		_, err := tx.ExecContext(ctx,
			`INSERT INTO actor_registry
			   (actor_id, actor_kind, actor_binding, host, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, ?, NULL)`,
			string(add.ID), string(add.Kind), nullableString(string(add.Binding)), add.Host, add.At,
		)
		if err != nil {
			return false, fmt.Errorf("store: actor member insert %q: %w", add.ID, err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("store: actor member lookup %q: %w", add.ID, err)
	}
}

func (r *actorRegistry) applyMemberRemoveTx(ctx context.Context, tx *sql.Tx, remove storespec.MemberActorRemove) (bool, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`,
		remove.At, string(remove.ID),
	)
	if err != nil {
		return false, fmt.Errorf("store: actor member deregister %q: %w", remove.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		// Do NOT swallow (same law as Deregister above): with n unknowable,
		// falling into the no-op branch would let the enclosing tx COMMIT a
		// deregistered_at that took effect while skipping the state/timer
		// cascade and the mirror event — a silently half-applied removal.
		return false, fmt.Errorf("store: actor member deregister rows-affected %q: %w", remove.ID, err)
	}
	if n != 1 {
		// Already-deregistered / missing member: idempotent no-op, no cascade (a
		// re-run must not re-clear).
		return false, nil
	}
	// Deregistration took effect this tx — cascade-clear the actor's state in the
	// same tx (scope law), atomic with the deregistered_at write.
	if err := clearActorScopedTx(ctx, tx, remove.ID); err != nil {
		return false, err
	}
	// Same tx: cascade-clear its identity-level pending timers, parallel to the
	// state cascade above.
	if err := clearTimersTx(ctx, tx, remove.ID); err != nil {
		return false, err
	}
	return true, nil
}

// Mirror event IDs are random uuids, the same as every ordinary envelope —
// deliberately NOT deterministic. Determinism would be load-bearing only if
// mirror appends had a cross-tx replay window to dedup (the timer fire id's
// situation: append and row-delete are two operations, a crash between them
// replays the append). A mirror append has no such window: it commits in the
// SAME tx as the registry transition, and a replayed batch takes the
// changed=false arm and appends nothing — idempotency lives in the registry
// state guard, not in an id collision. The former deterministic
// <type>:<actor>:<at> id carried no idempotency and had one runtime effect:
// a same-millisecond remove→re-add collided with the FIRST registration's
// mirror on messages.id UNIQUE and rolled back the whole re-add (a
// deterministic-At replayer — reconcile re-laying a member plan, a seeded
// boot — would fail permanently).
func actorRegisteredEnvelope(channelID channel.ID, add storespec.MemberActorAdd) *message.Envelope {
	payload, _ := json.Marshal(map[string]any{
		"actor_id":      add.ID,
		"actor_kind":    add.Kind,
		"actor_binding": add.Binding,
		"registered_at": add.At,
	})
	return &message.Envelope{
		ID:         message.ID(uuid.NewString()),
		TS:         add.At,
		TSReceived: add.At,
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       actor.ReservedSystemActorRegistered,
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
}

func actorDeregisteredEnvelope(channelID channel.ID, remove storespec.MemberActorRemove) *message.Envelope {
	payload, _ := json.Marshal(map[string]any{
		"actor_id":        remove.ID,
		"deregistered_at": remove.At,
	})
	return &message.Envelope{
		ID:         message.ID(uuid.NewString()), // random, same law as registered (see above)
		TS:         remove.At,
		TSReceived: remove.At,
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       actor.ReservedSystemActorDeregistered,
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
}
