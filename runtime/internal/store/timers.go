package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
	if class != timerspec.DeathFireRejected {
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

// FireAndMark commits the fire truth and pending→fired transition together.
// A missing row means Cancel won; an already-fired row is the idempotent retry
// hit after a lost commit response.
func (s *timerStore) FireAndMark(ctx context.Context, id timerspec.TimerID, env *message.Envelope) (timerspec.FireOutcome, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM timers WHERE timer_id=?`, string(id)).Scan(&state)
	if err == sql.ErrNoRows {
		return timerspec.FireCancelled, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: timer fire lookup %q: %w", id, err)
	}
	if state == "fired" {
		return timerspec.FireAlreadyFired, nil
	}
	if state != "pending" {
		return 0, fmt.Errorf("store: timer %q invalid state %q", id, state)
	}
	if _, err := appendTx(ctx, tx, env, false); err != nil {
		return 0, fmt.Errorf("store: timer fire append %q: %w", id, err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE timers SET state='fired' WHERE timer_id=? AND state='pending'`, string(id))
	if err != nil {
		return 0, fmt.Errorf("store: timer fire mark %q: %w", id, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return 0, fmt.Errorf("store: timer fire mark %q affected %d: %w", id, n, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if s.onCommit != nil {
		s.onCommit()
	}
	return timerspec.FireCommitted, nil
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

func (s *timerStore) ListFired(ctx context.Context, cursor timerspec.FiredCursor, limit int) (timerspec.FiredPage, error) {
	if limit <= 0 {
		limit = 256
	}
	if limit > 1024 {
		limit = 1024
	}
	rows, err := s.db.QueryContext(ctx, `SELECT timer_id, author_id, fire_at, type, payload,
		COALESCE(correlation_id,''), created_at FROM timers
		WHERE state='fired' AND timer_id>? ORDER BY timer_id LIMIT ?`, string(cursor.After), limit+1)
	if err != nil {
		return timerspec.FiredPage{}, fmt.Errorf("store: list fired timers: %w", err)
	}
	defer rows.Close()
	page := timerspec.FiredPage{Done: true}
	for rows.Next() {
		var row timerspec.TimerRow
		var id, author string
		if err := rows.Scan(&id, &author, &row.FireAt, &row.Type, &row.Payload, &row.CorrelationID, &row.CreatedAt); err != nil {
			return timerspec.FiredPage{}, err
		}
		row.ID, row.AuthorID = timerspec.TimerID(id), actor.ActorID(author)
		page.Rows = append(page.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return timerspec.FiredPage{}, err
	}
	if len(page.Rows) > limit {
		page.Rows = page.Rows[:limit]
		page.Done = false
	}
	if len(page.Rows) > 0 {
		page.Next.After = page.Rows[len(page.Rows)-1].ID
	}
	return page, nil
}

// clearTimersTx cascades the identity-level pending-timer locus: it deletes
// every timers row owned by author, inside the same transaction that
// deregisters the actor (both dereg entry points in actors.go hang it
// there). It is a parallel sibling of clearActorScopedTx (state.go), never
// merged into it — one locus, one function — so a future third scoped locus
// finds its own cascade in its own file. Idempotent: a re-run over an
// already-cleared author deletes zero rows.
//
// Memory-home timers need no SQL hook here because they are not rows; they
// live in the current Scheduler instance. Deregister makes subsequent fire
// admission fail by ActorID, so those entries are reaped lazily at fire time.
// This storage distinction is unrelated to actor Incarnation.
func clearTimersTx(ctx context.Context, tx *sql.Tx, author actor.ActorID) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM timers WHERE author_id=?`, string(author)); err != nil {
		return fmt.Errorf("store: timers cascade clear %q: %w", author, err)
	}
	return nil
}

var _ timerspec.TimerStore = (*timerStore)(nil)
var _ timerspec.TimerFireStore = (*timerStore)(nil)
