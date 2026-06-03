package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// actorRegistry implements kernel/storespec.Registry over a channel-local
// sqlite. Each *actorRegistry is bound to one channel database.
type actorRegistry struct {
	db *sql.DB
}

// NewActorRegistry returns a registry over the given channel sqlite.
// (v2: no fence / outbox — single writer by construction; the v1
// framework-owned same-tx projection is removed.)
func newActorRegistry(db *sql.DB) *actorRegistry { return &actorRegistry{db: db} }

// Lookup implements storespec.Registry.
func (r *actorRegistry) Lookup(ctx context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                 created_at, COALESCE(deregistered_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec storespec.Record
	var kind, binding string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &binding, &rec.CreatedAt, &rec.DeregisteredAt,
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
	rec.Binding = actor.Binding(binding)
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
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
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
		if err := rows.Scan(&rec.ID, &kind, &binding, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		k, ok := actor.ParseKind(kind)
		if !ok {
			return nil, fmt.Errorf("store: list active actors invalid kind %q (out of closed set)", kind)
		}
		rec.Kind = k
		rec.Binding = actor.Binding(binding)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active actors rows: %w", err)
	}
	return out, nil
}

// Insert implements storespec.Registry: it adds one membership row.
func (r *actorRegistry) Insert(ctx context.Context, rec storespec.Record) error {
	if rec.ID == "" {
		return errors.New("store: actor insert: empty ID")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor insert begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insActor = `INSERT INTO actor_registry
	   (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
	   VALUES (?, ?, ?, ?, NULL)`
	var binding any
	if rec.Binding == "" {
		binding = nil
	} else {
		binding = string(rec.Binding)
	}
	if _, err := tx.ExecContext(ctx, insActor,
		string(rec.ID), string(rec.Kind), binding, rec.CreatedAt,
	); err != nil {
		return fmt.Errorf("store: actor insert %q: %w", rec.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor insert commit: %w", err)
	}
	return nil
}

// Deregister implements storespec.Registry.
func (r *actorRegistry) Deregister(ctx context.Context, id actor.ActorID, at int64) error {
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

// Membership transition DTOs (storespec.MemberActorAdd / storespec.MemberActorProxyHost /
// storespec.MemberActorRemove) + the MembershipControlPlane contract live in
// runtime/storespec (contract types, §4.5). This file is their sqlite
// implementation.

// ApplyMemberTransitions mutates actor_registry and appends the matching
// system.actor.* mirror events in one sqlite transaction. Duplicate retries
// are idempotent: already-active adds and already-deregistered removes do not
// append a second event.
func (r *actorRegistry) ApplyMemberTransitions(
	ctx context.Context,
	channelID channel.ID,
	adds []storespec.MemberActorAdd,
	removes []storespec.MemberActorRemove,
) error {
	if channelID == "" {
		return errors.New("store: actor member transition: empty channel_id")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: actor member transition begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

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
		if _, err := appendTx(ctx, tx, env, false); err != nil {
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
		if _, err := appendTx(ctx, tx, env, false); err != nil {
			return fmt.Errorf("store: actor deregistered mirror %q: %w", remove.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: actor member transition commit: %w", err)
	}
	return nil
}

func (r *actorRegistry) applyMemberAddTx(ctx context.Context, tx *sql.Tx, add storespec.MemberActorAdd) (bool, error) {
	var deregistered sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT deregistered_at FROM actor_registry WHERE actor_id=?`, string(add.ID)).Scan(&deregistered)
	switch {
	case err == nil:
		if !deregistered.Valid {
			// Row is already active (reconnect / retried update_members /
			// re-run of a boot-time seed). A duplicate add is an idempotent
			// no-op: substrate identity is {ID, Kind, Binding} and carries no
			// per-actor declaration to diff. An actor's capability/service
			// declaration is an application-level event (skill-as-document,
			// runtime), not a substrate membership field.
			return false, nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE actor_registry
			    SET actor_kind=?, actor_binding=?, created_at=?, deregistered_at=NULL
			  WHERE actor_id=?`,
			string(add.Kind), nullableString(string(add.Binding)), add.At, string(add.ID),
		)
		if err != nil {
			return false, fmt.Errorf("store: actor reactivate %q: %w", add.ID, err)
		}
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		_, err := tx.ExecContext(ctx,
			`INSERT INTO actor_registry
			   (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, NULL)`,
			string(add.ID), string(add.Kind), nullableString(string(add.Binding)), add.At,
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
		return false, err
	}
	return n == 1, nil
}

func actorRegisteredEnvelope(channelID channel.ID, add storespec.MemberActorAdd) (*message.Envelope, error) {
	payloadMap := map[string]any{
		"actor_id":      add.ID,
		"actor_kind":    add.Kind,
		"actor_binding": add.Binding,
		"registered_at": add.At,
	}
	if add.ProxyHost.DaemonID != "" {
		payloadMap["proxy_host"] = map[string]any{
			"daemon_id": add.ProxyHost.DaemonID,
		}
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return nil, err
	}
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("%s:%s:%d", actor.ReservedSystemActorRegistered, add.ID, add.At)),
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
	return env, nil
}

func actorDeregisteredEnvelope(channelID channel.ID, remove storespec.MemberActorRemove) (*message.Envelope, error) {
	payload, err := json.Marshal(map[string]any{
		"actor_id":        remove.ID,
		"deregistered_at": remove.At,
	})
	if err != nil {
		return nil, err
	}
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("%s:%s:%d", actor.ReservedSystemActorDeregistered, remove.ID, remove.At)),
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
	return env, nil
}
