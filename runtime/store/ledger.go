package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/ledger"
)

// Ledger implements kernel/ledger.Ledger over the action_ledger table.
type Ledger struct {
	db *sql.DB
}

// NewLedger returns a Ledger bound to the channel sqlite.
func NewLedger(db *sql.DB) *Ledger { return &Ledger{db: db} }

// Find implements ledger.Ledger.
func (l *Ledger) Find(ctx context.Context, key ledger.Key) (ledger.Entry, bool, error) {
	const q = `SELECT ledger_key, turn_id, actor_id, envelope_id, status,
	                 reserved_at, COALESCE(committed_at,0)
	            FROM action_ledger WHERE ledger_key=?`
	var e ledger.Entry
	var status, actorID string
	err := l.db.QueryRowContext(ctx, q, string(key)).Scan(
		&e.Key, &e.TurnID, &actorID, &e.EnvelopeID, &status, &e.ReservedAt, &e.CommittedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ledger.Entry{}, false, nil
	}
	if err != nil {
		return ledger.Entry{}, false, fmt.Errorf("store: ledger find: %w", err)
	}
	e.ActorID = actor.ActorID(actorID)
	e.Status = ledger.Status(status)
	return e, true, nil
}

// Reserve implements ledger.Ledger — idempotent INSERT-or-return-existing.
// Caller picks envelopeID; if a row already exists for the key the
// existing entry is returned unchanged (envelope_id from first reserve
// wins per L2 §1.4.10.1).
func (l *Ledger) Reserve(ctx context.Context, e ledger.Entry) (ledger.Entry, error) {
	if e.Key == "" {
		return ledger.Entry{}, errors.New("store: ledger reserve: empty key")
	}
	if e.EnvelopeID == "" {
		return ledger.Entry{}, errors.New("store: ledger reserve: empty envelope_id")
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return ledger.Entry{}, fmt.Errorf("store: ledger reserve begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const ins = `INSERT OR IGNORE INTO action_ledger
	   (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
	   VALUES (?, ?, ?, ?, 'reserved', ?, NULL)`
	if _, err := tx.ExecContext(ctx, ins,
		string(e.Key), e.TurnID, string(e.ActorID), e.EnvelopeID, e.ReservedAt,
	); err != nil {
		return ledger.Entry{}, fmt.Errorf("store: ledger reserve insert: %w", err)
	}

	const sel = `SELECT ledger_key, turn_id, actor_id, envelope_id, status,
	                    reserved_at, COALESCE(committed_at,0)
	             FROM action_ledger WHERE ledger_key=?`
	var got ledger.Entry
	var status, actorID string
	if err := tx.QueryRowContext(ctx, sel, string(e.Key)).Scan(
		&got.Key, &got.TurnID, &actorID, &got.EnvelopeID, &status,
		&got.ReservedAt, &got.CommittedAt,
	); err != nil {
		return ledger.Entry{}, fmt.Errorf("store: ledger reserve select: %w", err)
	}
	got.ActorID = actor.ActorID(actorID)
	got.Status = ledger.Status(status)

	if err := tx.Commit(); err != nil {
		return ledger.Entry{}, fmt.Errorf("store: ledger reserve commit: %w", err)
	}
	return got, nil
}

// Commit implements ledger.Ledger — idempotent CAS to committed.
func (l *Ledger) Commit(ctx context.Context, key ledger.Key, committedAt int64) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: ledger commit begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Ensure the row exists; missing row → caller bug.
	const sel = `SELECT status FROM action_ledger WHERE ledger_key=?`
	var status string
	switch err := tx.QueryRowContext(ctx, sel, string(key)).Scan(&status); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("store: ledger commit missing key %q", key)
	case err != nil:
		return fmt.Errorf("store: ledger commit select: %w", err)
	}

	const upd = `UPDATE action_ledger
	             SET status='committed', committed_at=?
	             WHERE ledger_key=? AND status='reserved'`
	if _, err := tx.ExecContext(ctx, upd, committedAt, string(key)); err != nil {
		return fmt.Errorf("store: ledger commit update: %w", err)
	}
	// status==committed already → no rows affected; treat as idempotent ok.

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: ledger commit tx: %w", err)
	}
	return nil
}
