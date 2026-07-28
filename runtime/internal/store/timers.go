package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// timerStore implements timerspec.TimerStore over the channel-local `timers`
// table — the durable half of the time axis: control-plane pending intent,
// keyed by author ActorID, NEVER truth. Bound to one channel database, the same
// locus discipline every other channel-local store follows. It trusts its
// caller (the schedule capability admits and welds author before Insert;
// store-not-validate) and is itself confined to package store — the runtime
// tree assembles it behind ChannelStores' unexported field.
type timerStore struct {
	db       *sql.DB
	onCommit func()
}

// maxPendingTimersPerAuthor / maxDeadTimers are vars ONLY so same-package
// tests can shrink them and exercise the quota/ring SEMANTICS without
// physically inserting thousands of fsync'd rows (the ring-eviction test
// alone burned 34s at production size); production never writes them, and
// the production values are pinned by TestTimerCapsProductionValues.
var (
	maxPendingTimersPerAuthor = 1024
	maxDeadTimers             = 4096
)

const duePerAuthor = 32

func newTimerStore(db *sql.DB, callbacks ...func()) *timerStore {
	var onCommit func()
	if len(callbacks) > 0 {
		onCommit = callbacks[0]
	}
	return &timerStore{db: db, onCommit: onCommit}
}

// Insert adds one pending row. The engine mints a fresh TimerID per Schedule
// and never reuses one (timerspec.TimerID doc), so a PRIMARY KEY collision
// here signals a minting bug, not a legitimate caller race — it is surfaced
// as an ordinary store error, never swallowed into a collision sentinel
// (unlike resources/actor_state Create, which model a caller-supplied id that
// legitimately races).
func (s *timerStore) Insert(ctx context.Context, row timerspec.TimerRow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var pending int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE author_id=?`, string(row.AuthorID)).Scan(&pending); err != nil {
		return err
	}
	if pending >= maxPendingTimersPerAuthor {
		return timerspec.ErrScheduleQuota
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO timers (timer_id, author_id, fire_at, type, payload, correlation_id, created_at, state)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')`,
		string(row.ID), string(row.AuthorID), row.FireAt, row.Type, row.Payload,
		nullableString(row.CorrelationID), row.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: timer insert %q: %w", row.ID, err)
	}
	return tx.Commit()
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

// Due returns rows with fire_at <= now, ordered by fire_at and bounded only by
// the per-author SQL window. There is deliberately no cross-author page limit.
func (s *timerStore) Due(ctx context.Context, now int64) ([]timerspec.TimerRow, error) {
	const q = `SELECT timer_id, author_id, fire_at, type, payload, COALESCE(correlation_id, ''), created_at FROM (
	             SELECT *, ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY fire_at, timer_id) AS rn
	             FROM timers WHERE state='pending' AND fire_at <= ?) WHERE rn <= ? ORDER BY fire_at, timer_id`
	rows, err := s.db.QueryContext(ctx, q, now, duePerAuthor)
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

func (s *timerStore) MoveToDead(ctx context.Context, id timerspec.TimerID, class timerspec.DeathClass, reason, detail string, diedAt int64) (bool, int, error) {
	// Explicit closed-set check: adding a DeathClass value means adding a case
	// here, so a new class is admitted deliberately instead of being silently
	// rejected by an equality against the (currently single) legal value.
	switch class {
	case timerspec.DeathFireRejected:
	default:
		return false, 0, fmt.Errorf("store: invalid timer death class %q", class)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `INSERT INTO timer_dead (timer_id,author_id,fire_at,type,payload,correlation_id,created_at,death_class,reason,detail,died_at)
	 SELECT timer_id,author_id,fire_at,type,payload,correlation_id,created_at,?,?,?,? FROM timers WHERE timer_id=?`, string(class), reason, detail, diedAt, string(id))
	if err != nil {
		return false, 0, err
	}
	n, err := res.RowsAffected()
	if err != nil || n > 1 {
		return false, 0, fmt.Errorf("store: timer dead insert affected %d: %w", n, err)
	}
	if n == 0 {
		return false, 0, nil
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM timers WHERE timer_id=?`, string(id)); err != nil {
		return false, 0, err
	}
	res, err = tx.ExecContext(ctx, `DELETE FROM timer_dead WHERE dead_seq IN (SELECT dead_seq FROM timer_dead ORDER BY dead_seq DESC LIMIT -1 OFFSET ?)`, maxDeadTimers)
	if err != nil {
		return false, 0, err
	}
	evicted64, err := res.RowsAffected()
	if err != nil {
		return false, 0, err
	}
	if err = tx.Commit(); err != nil {
		return false, 0, err
	}
	return true, int(evicted64), nil
}

// NextFireAt returns the earliest pending fire_at (ok=false when the table is
// empty) — the poll/wake loop's sleep-until target. Walks ix_timers_fire_at
// via MIN().
func (s *timerStore) NextFireAt(ctx context.Context) (int64, bool, error) {
	const q = `SELECT MIN(fire_at) FROM timers WHERE state='pending'`
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
		`DELETE FROM timers WHERE timer_id=? AND author_id=? AND state='pending'`,
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

// MarkFired advances only timer control state. Message truth has already
// passed the ordinary Harness and committed under its deterministic ID.
func (s *timerStore) MarkFired(ctx context.Context, id timerspec.TimerID) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE timers SET state='fired' WHERE timer_id=? AND state='pending'`,
		string(id),
	)
	if err != nil {
		return fmt.Errorf("store: timer mark fired %q: %w", id, err)
	}
	if _, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("store: timer mark fired rows %q: %w", id, err)
	}
	return nil
}

func (s *timerStore) AckOwned(ctx context.Context, id timerspec.TimerID, author actor.ActorID) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id=? AND author_id=? AND state='fired'`, string(id), string(author))
	if err != nil {
		return false, fmt.Errorf("store: timer ack %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: timer ack rows %q: %w", id, err)
	}
	return n == 1, nil
}

// No deregistration cascade clears this table. A dead author's timer rows are
// inert data — ActorIDs are never reused and every belonging is keyed by
// ActorID, so those rows are unreachable to anyone but the dead. Correctness is
// carried by the fire-time author admission gate (which refuses and reaps),
// never by a delete.

var _ timerspec.TimerStore = (*timerStore)(nil)
