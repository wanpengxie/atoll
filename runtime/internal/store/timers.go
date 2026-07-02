package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/timerspec"
)

// timerStore implements timerspec.TimerStore over the channel-local `timers`
// table — the durable IDENTITY-level half of the time axis (schema.go §6):
// control-plane pending intent, keyed by a durable name (author identity),
// NEVER truth. Bound to one channel database, the same locus discipline every
// other channel-local store follows. It trusts its caller (the schedule
// engine welds author before Insert; store-not-validate, mirrors
// resourceRegistry/stateStore) and is itself CONFINED to package store — the
// runtime tree assembles it behind ChannelStores' unexported field (红线❻).
type timerStore struct {
	db *sql.DB
}

func newTimerStore(db *sql.DB) *timerStore {
	return &timerStore{db: db}
}

// Insert adds one pending row. The engine mints a fresh TimerID per Schedule
// and never reuses one (timerspec.TimerID doc), so a PRIMARY KEY collision
// here signals a minting bug, not a legitimate caller race — it is surfaced
// as an ordinary store error, never swallowed into a collision sentinel
// (unlike resources/actor_state Create, which model a caller-supplied id that
// legitimately races).
func (s *timerStore) Insert(ctx context.Context, row timerspec.TimerRow) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO timers (timer_id, author_id, fire_at, type, payload, correlation_id, created_at)
		   VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(row.ID), string(row.AuthorID), row.FireAt, row.Type, row.Payload,
		nullableString(row.CorrelationID), row.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: timer insert %q: %w", row.ID, err)
	}
	return nil
}

// Delete removes one pending row (fire completion / cancel / drop). A missing
// row is honestly existed=false, not an error — Cancel after fire is a no-op
// (fired truth is not retractable) and re-deleting an already-completed row
// is idempotent.
func (s *timerStore) Delete(ctx context.Context, id timerspec.TimerID) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id=?`, string(id))
	if err != nil {
		return false, fmt.Errorf("store: timer delete %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: timer delete rows-affected %q: %w", id, err)
	}
	return n > 0, nil
}

// Due returns rows with fire_at <= now, ordered by fire_at, capped at limit —
// the engine's per-tick batch of identity-bind rows to fire. Walks
// ix_timers_fire_at.
func (s *timerStore) Due(ctx context.Context, now int64, limit int) ([]timerspec.TimerRow, error) {
	const q = `SELECT timer_id, author_id, fire_at, type, payload, COALESCE(correlation_id, ''), created_at
	             FROM timers WHERE fire_at <= ? ORDER BY fire_at LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("store: timers due: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []timerspec.TimerRow
	for rows.Next() {
		var id, author, typ, corr string
		var row timerspec.TimerRow
		if err := rows.Scan(&id, &author, &row.FireAt, &typ, &row.Payload, &corr, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: timers due scan: %w", err)
		}
		row.ID = timerspec.TimerID(id)
		row.AuthorID = actor.ActorID(author)
		row.Type = typ
		row.CorrelationID = corr
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: timers due rows: %w", err)
	}
	return out, nil
}

// NextFireAt returns the earliest pending fire_at (ok=false when the table is
// empty) — the poll/wake loop's sleep-until target. Walks ix_timers_fire_at
// via MIN().
func (s *timerStore) NextFireAt(ctx context.Context) (int64, bool, error) {
	const q = `SELECT MIN(fire_at) FROM timers`
	var fireAt sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q).Scan(&fireAt); err != nil {
		return 0, false, fmt.Errorf("store: timers next-fire-at: %w", err)
	}
	if !fireAt.Valid {
		return 0, false, nil
	}
	return fireAt.Int64, true, nil
}

// CancelOwned deletes id IFF its author matches — the non-ambient check lives
// in the same statement (author is in the WHERE clause), so a handle can only
// ever cancel its own timers. A foreign or absent id is the same
// existed=false, never leaking whether some OTHER author's timer exists.
func (s *timerStore) CancelOwned(ctx context.Context, id timerspec.TimerID, author actor.ActorID) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM timers WHERE timer_id=? AND author_id=?`,
		string(id), string(author),
	)
	if err != nil {
		return false, fmt.Errorf("store: timer cancel-owned %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: timer cancel-owned rows-affected %q: %w", id, err)
	}
	return n > 0, nil
}

// clearTimersTx cascades the identity-level pending-timer locus: it deletes
// every timers row owned by author, inside the SAME transaction that
// deregisters the actor (both dereg entry points in actors.go hang it there,
// §10.12 row 6). It is a PARALLEL sibling of clearActorScopedTx (state.go),
// never merged into it — one locus, one function (v1.2 opus-nit) — so a
// future third scoped locus finds its own cascade in its own file.
// Idempotent: a re-run over an already-cleared author deletes zero rows.
//
// Incarnation-bind timers need NO hook here — they are not rows (they live in
// the schedule engine's in-memory due-set, welded to the live embodiment,
// v1.1 历史校准). Deregister implies the embodiment already died, so those
// entries are reaped lazily at fire time via IsLive (§1.3) — zero coupling to
// this cascade.
func clearTimersTx(ctx context.Context, tx *sql.Tx, author actor.ActorID) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE author_id=?`, string(author)); err != nil {
		return fmt.Errorf("store: timers cascade clear %q: %w", author, err)
	}
	return nil
}

var _ timerspec.TimerStore = (*timerStore)(nil)
