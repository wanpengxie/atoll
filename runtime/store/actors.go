package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ActorRegistry implements kernel/actor.Registry over a channel-local
// sqlite. Each *ActorRegistry is bound to one channel database.
type ActorRegistry struct {
	db *sql.DB
}

// NewActorRegistry returns a registry over the given channel sqlite.
func NewActorRegistry(db *sql.DB) *ActorRegistry { return &ActorRegistry{db: db} }

// Lookup implements actor.Registry.
func (r *ActorRegistry) Lookup(ctx context.Context, id actor.ActorID) (actor.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, COALESCE(actor_binding,''),
	                 COALESCE(display_name,''), created_at,
	                 COALESCE(deregistered_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec actor.Record
	var kind, binding string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt, &rec.DeregisteredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return actor.Record{}, false, nil
	}
	if err != nil {
		return actor.Record{}, false, fmt.Errorf("store: actor lookup %q: %w", id, err)
	}
	rec.Kind = message.SenderKind(kind)
	rec.Binding = actor.Binding(binding)
	return rec, true, nil
}

// Exists implements actor.Registry — returns true even for soft-deregistered.
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

// ListActive implements actor.Registry.
func (r *ActorRegistry) ListActive(ctx context.Context) ([]actor.Record, error) {
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

	var out []actor.Record
	for rows.Next() {
		var rec actor.Record
		var kind, binding string
		if err := rows.Scan(&rec.ID, &kind, &binding, &rec.DisplayName, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		rec.Kind = message.SenderKind(kind)
		rec.Binding = actor.Binding(binding)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active actors rows: %w", err)
	}
	return out, nil
}

// Insert implements actor.Registry. Per L2 §1.4.6 invariant, the
// actor_cursors row is seeded in the same transaction.
func (r *ActorRegistry) Insert(ctx context.Context, rec actor.Record) error {
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

// Deregister implements actor.Registry.
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
