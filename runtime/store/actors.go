package store

import (
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
	db *sql.DB
}

// NewActorRegistry returns a registry over the given channel sqlite.
func NewActorRegistry(db *sql.DB) *ActorRegistry { return &ActorRegistry{db: db} }

// Lookup implements actorreg.Registry.
func (r *ActorRegistry) Lookup(ctx context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                 COALESCE(display_name,''), created_at,
	                 COALESCE(deregistered_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec actorreg.Record
	var kind, binding string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt, &rec.DeregisteredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return actorreg.Record{}, false, nil
	}
	if err != nil {
		return actorreg.Record{}, false, fmt.Errorf("store: actor lookup %q: %w", id, err)
	}
	rec.Kind = actor.Kind(kind)
	rec.Binding = actor.Binding(binding)
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
	                 COALESCE(display_name,''), created_at
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
		var kind, binding string
		if err := rows.Scan(&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		rec.Kind = actor.Kind(kind)
		rec.Binding = actor.Binding(binding)
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

	const insActor = `INSERT INTO actor_registry
	   (actor_id, actor_kind, actor_binding, display_name, created_at, deregistered_at)
	   VALUES (?, ?, ?, ?, ?, NULL)`
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

func (r *ActorRegistry) applyMemberAddTx(ctx context.Context, tx *sql.Tx, add MemberActorAdd) (bool, error) {
	var deregistered sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT deregistered_at FROM actor_registry WHERE actor_id=?`, string(add.ID)).Scan(&deregistered)
	switch {
	case err == nil:
		if !deregistered.Valid {
			return false, nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE actor_registry
			    SET actor_kind=?, actor_binding=?, display_name=?, created_at=?, deregistered_at=NULL
			  WHERE actor_id=?`,
			string(add.Kind), nullableString(string(add.Binding)), nullableString(add.DisplayName), add.At, string(add.ID),
		)
		if err != nil {
			return false, fmt.Errorf("store: actor reactivate %q: %w", add.ID, err)
		}
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		_, err := tx.ExecContext(ctx,
			`INSERT INTO actor_registry
			   (actor_id, actor_kind, actor_binding, display_name, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, ?, NULL)`,
			string(add.ID), string(add.Kind), nullableString(string(add.Binding)), nullableString(add.DisplayName), add.At,
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

func actorRegisteredEnvelope(channelID channel.ID, add MemberActorAdd) (*message.Envelope, error) {
	payload, err := json.Marshal(map[string]any{
		"actor_id":      add.ID,
		"actor_kind":    add.Kind,
		"actor_binding": add.Binding,
		"display_name":  add.DisplayName,
		"user_id":       add.UserID,
		"role":          add.Role,
		"registered_at": add.At,
	})
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
		Audience:   message.Audience{message.AudienceWildcard},
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
		Audience:   message.Audience{message.AudienceWildcard},
	}
	hash, err := message.CanonicalHash(*env)
	env.CanonicalHash = hash
	return env, err
}
