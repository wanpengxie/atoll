package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ledgerStore implements storespec.Ledger over the action_ledger table.
// (v2: no fencing — single writer by construction.)
type ledgerStore struct {
	db *sql.DB
}

// NewLedger returns a ledgerStore over the given channel sqlite.
func newLedger(db *sql.DB) *ledgerStore { return &ledgerStore{db: db} }

// Find implements storespec.Ledger.
func (l *ledgerStore) Find(ctx context.Context, key ledger.Key) (storespec.Entry, bool, error) {
	const q = `SELECT ledger_key, turn_id, actor_id, envelope_id, status,
	                 reserved_at, COALESCE(committed_at,0)
	            FROM action_ledger WHERE ledger_key=?`
	var e storespec.Entry
	var status, actorID string
	err := l.db.QueryRowContext(ctx, q, string(key)).Scan(
		&e.Key, &e.TurnID, &actorID, &e.EnvelopeID, &status, &e.ReservedAt, &e.CommittedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.Entry{}, false, nil
	}
	if err != nil {
		return storespec.Entry{}, false, fmt.Errorf("store: ledger find: %w", err)
	}
	e.ActorID = actor.ActorID(actorID)
	e.Status = storespec.Status(status)
	return e, true, nil
}

// Reserve implements storespec.Ledger — idempotent INSERT-or-return-existing.
// Caller picks envelopeID; if a row already exists for the key the
// existing entry is returned unchanged (envelope_id from first reserve
// wins per L2 §1.4.10.1).
func (l *ledgerStore) Reserve(ctx context.Context, e storespec.Entry) (storespec.Entry, error) {
	if e.Key == "" {
		return storespec.Entry{}, errors.New("store: ledger reserve: empty key")
	}
	if e.EnvelopeID == "" {
		return storespec.Entry{}, errors.New("store: ledger reserve: empty envelope_id")
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.Entry{}, fmt.Errorf("store: ledger reserve begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const ins = `INSERT OR IGNORE INTO action_ledger
	   (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
	   VALUES (?, ?, ?, ?, 'reserved', ?, NULL)`
	if _, err := tx.ExecContext(ctx, ins,
		string(e.Key), e.TurnID, string(e.ActorID), e.EnvelopeID, e.ReservedAt,
	); err != nil {
		return storespec.Entry{}, fmt.Errorf("store: ledger reserve insert: %w", err)
	}

	const sel = `SELECT ledger_key, turn_id, actor_id, envelope_id, status,
	                    reserved_at, COALESCE(committed_at,0)
	             FROM action_ledger WHERE ledger_key=?`
	var got storespec.Entry
	var status, actorID string
	if err := tx.QueryRowContext(ctx, sel, string(e.Key)).Scan(
		&got.Key, &got.TurnID, &actorID, &got.EnvelopeID, &status,
		&got.ReservedAt, &got.CommittedAt,
	); err != nil {
		return storespec.Entry{}, fmt.Errorf("store: ledger reserve select: %w", err)
	}
	got.ActorID = actor.ActorID(actorID)
	got.Status = storespec.Status(status)

	if err := tx.Commit(); err != nil {
		return storespec.Entry{}, fmt.Errorf("store: ledger reserve commit: %w", err)
	}
	return got, nil
}

// Commit implements storespec.Ledger — idempotent CAS to committed.
func (l *ledgerStore) Commit(ctx context.Context, key ledger.Key, committedAt int64) error {
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
