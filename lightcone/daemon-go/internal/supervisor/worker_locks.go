// Package supervisor implements the worker_locks CAS + supervisor main
// loop + backlog scan per L2 §1.4.9 / §1.4.10 (M1.3 ticket T6).
//
// Three primitives live in this file (worker_locks.go):
//
//   - Acquire:   CAS row INSERT or steal — increments fencing_token on
//     steal so any old worker that survives is fenced out at
//     harness write time (L2 §1.4.9 spawn protocol).
//   - Heartbeat: CAS UPDATE that extends the lease as long as the caller
//     holds the (worker_id, fencing_token) pair — a steal
//     bumps the token, so the old worker's Heartbeat returns
//     ErrFencingStale and self-destructs.
//   - Release:   self-DELETE (caller proves ownership via worker_id),
//     used both on graceful shutdown and after a failed spawn.
//
// All three are designed to compose with internal/store.WithImmediate
// — they accept either the *sql.DB pool (Acquire wraps WithImmediate
// internally) or an Executor that the caller already opened a tx on
// (Heartbeat / Release are tx-friendly so the supervisor loop can
// batch them with backlog scans or ledger writes).
package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// DefaultLeaseTTL is the protocol-baseline lease lifetime per L2 §1.4.9
// ("lease_ttl 默认值：60 秒"). Daemon callers may override per channel
// config (M1.x); the supervisor Loop accepts a custom value via
// LoopConfig.
const DefaultLeaseTTL int64 = 60

// Lock is the in-memory representation of one worker_locks row. The
// columns match L2 §1.4.9 DDL exactly.
type Lock struct {
	AgentID        string
	WorkerID       string
	FencingToken   int64
	LeaseExpiresAt int64 // Unix seconds
	AcquiredAt     int64 // Unix seconds
}

// Expired reports whether the lock is past its lease at the given wall
// clock. Equality is treated as expired — matches the spec CAS predicate
// `lease_expires_at <= :now`.
func (l Lock) Expired(now int64) bool { return l.LeaseExpiresAt <= now }

// Executor is the smallest database/sql interface that supports the
// worker_locks read + write paths. *sql.DB, *sql.Conn, and *sql.Tx all
// satisfy it.
//
// Acquire wraps WithImmediate internally because the protocol REQUIRES
// SELECT-then-(INSERT|UPDATE) atomicity. Heartbeat / Release / Get are
// stateless single-statement helpers — callers join them to whatever
// outer tx makes sense for their flow.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Sentinel errors callers inspect to drive the supervisor state machine
// and to surface to harness step 1 / install API consumers (L2 §3.6.1).
var (
	// ErrLockHeld is returned when Acquire finds a row whose lease is
	// still in the future (someone else owns the lock). Maps to
	// install reason `worker_lock_held` (HTTP 409).
	ErrLockHeld = errors.New("worker_lock_held")

	// ErrFencingStale is returned when Heartbeat's CAS misses — i.e.
	// the lock was stolen since the caller last refreshed. The old
	// worker MUST self-terminate per L2 §1.4.9 spawn protocol.
	// Maps to harness reject reason `worker_fencing_stale` (HTTP 410).
	ErrFencingStale = errors.New("worker_fencing_stale")

	// ErrLockMissing is returned when a Heartbeat / Release / Get
	// targets an agent that has no row at all (never acquired, or
	// already released). Differs from ErrFencingStale: stale means
	// "row exists, you're not the owner"; missing means "no row".
	ErrLockMissing = errors.New("worker_lock_missing")

	// ErrInvalidInput is returned when Acquire / Heartbeat / Release
	// receive empty agent_id / worker_id / non-positive now / TTL.
	ErrInvalidInput = errors.New("worker_locks_invalid_input")
)

// Acquire executes the L2 §1.4.9 "spawn protocol" in a single IMMEDIATE
// transaction. It is the only function in this file that takes *sql.DB
// directly — every other call site needs the read+write atomicity that
// BEGIN IMMEDIATE provides.
//
// Behaviour matrix (matches the spec 3-row table):
//
//	row absent             → INSERT fencing_token=1, return new Lock
//	row present, expired   → UPDATE worker_id/token+1/lease, return Lock
//	row present, not yet   → return ErrLockHeld (caller backs off)
//
// `now` returns Unix seconds; tests inject a fixed clock. `leaseTTL`
// is in seconds; pass DefaultLeaseTTL for protocol baseline.
func Acquire(
	ctx context.Context,
	db *sql.DB,
	agentID, workerID string,
	leaseTTL int64,
	now func() int64,
) (*Lock, error) {
	if err := validateAcquireInput(agentID, workerID, leaseTTL, now); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("%w: db is nil", ErrInvalidInput)
	}

	var lock *Lock
	err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		nowTS := now()
		existing, err := Get(ctx, conn, agentID)
		switch {
		case errors.Is(err, ErrLockMissing):
			// First-ever spawn for this agent. INSERT fencing_token=1.
			inserted, ierr := insertLock(ctx, conn, agentID, workerID, 1, nowTS, leaseTTL)
			if ierr != nil {
				return ierr
			}
			lock = inserted
			return nil
		case err != nil:
			return err
		}

		if !existing.Expired(nowTS) {
			// Old worker is still within its lease. Reject — caller
			// (supervisor loop) backs off until next tick.
			return ErrLockHeld
		}

		// Lease has expired — steal via CAS. The WHERE predicate
		// mirrors L2 §1.4.9 ("lease_expires_at <= :now") so concurrent
		// steal attempts serialise: whichever IMMEDIATE tx commits first
		// wins; the other observes `lease_expires_at > :now` on retry.
		stolen, serr := stealLock(ctx, conn, agentID, workerID, existing.FencingToken, nowTS, leaseTTL)
		if serr != nil {
			return serr
		}
		lock = stolen
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// Heartbeat extends the lease for an in-flight worker. The CAS
// predicate `worker_id = ? AND fencing_token = ?` guarantees that any
// worker which lost the steal race observes ErrFencingStale instead of
// silently extending a stolen lock.
//
// Callers normally pass the *sql.DB pool directly — heartbeats are
// single statement so there is no need to wrap them in WithImmediate.
// Pass a *sql.Conn or *sql.Tx if you need to batch the heartbeat with
// other writes.
//
// Returns:
//   - nil on success (lease extended)
//   - ErrFencingStale when the CAS misses (worker has been stolen)
//   - ErrLockMissing when no row exists for agentID
//   - wrapped sql error on driver / connection failures
func Heartbeat(
	ctx context.Context,
	exec Executor,
	agentID, workerID string,
	fencingToken, leaseTTL int64,
	now func() int64,
) error {
	if err := validateAcquireInput(agentID, workerID, leaseTTL, now); err != nil {
		return err
	}
	if fencingToken <= 0 {
		return fmt.Errorf("%w: fencing_token must be positive, got %d", ErrInvalidInput, fencingToken)
	}
	if exec == nil {
		return fmt.Errorf("%w: exec is nil", ErrInvalidInput)
	}
	nowTS := now()

	res, err := exec.ExecContext(ctx,
		`UPDATE worker_locks
		    SET lease_expires_at = ?
		  WHERE agent_id = ?
		    AND worker_id = ?
		    AND fencing_token = ?`,
		nowTS+leaseTTL, agentID, workerID, fencingToken,
	)
	if err != nil {
		return fmt.Errorf("worker_locks: heartbeat %s: %w", agentID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("worker_locks: heartbeat %s rowsAffected: %w", agentID, err)
	}
	if affected == 1 {
		return nil
	}

	// 0 rows — either the row vanished (Released by a prior step) or
	// the (worker_id, fencing_token) pair no longer matches because
	// somebody stole the lock. Distinguish the two so the caller can
	// log it precisely.
	current, gerr := Get(ctx, exec, agentID)
	if errors.Is(gerr, ErrLockMissing) {
		return ErrLockMissing
	}
	if gerr != nil {
		return gerr
	}
	// Row exists but our token/worker_id no longer matches → stale.
	_ = current
	return ErrFencingStale
}

// Release deletes the (agent_id, worker_id) row. The CAS protects
// against a stale worker re-Releasing a row that was stolen and re-
// acquired by someone else — Release only ever clears its own row.
//
// Returns:
//   - nil on success (row deleted)
//   - ErrLockMissing when no row matched (already released, or never
//     existed)
//   - wrapped sql error on driver failures
//
// Callers in the supervisor loop call Release on graceful shutdown and
// on spawn failure (the "release lock to avoid orphan" branch in
// L2 §1.4.10 pseudocode).
func Release(ctx context.Context, exec Executor, agentID, workerID string) error {
	if strings.TrimSpace(agentID) == "" {
		return fmt.Errorf("%w: agent_id required", ErrInvalidInput)
	}
	if strings.TrimSpace(workerID) == "" {
		return fmt.Errorf("%w: worker_id required", ErrInvalidInput)
	}
	if exec == nil {
		return fmt.Errorf("%w: exec is nil", ErrInvalidInput)
	}

	res, err := exec.ExecContext(ctx,
		`DELETE FROM worker_locks
		  WHERE agent_id = ? AND worker_id = ?`,
		agentID, workerID,
	)
	if err != nil {
		return fmt.Errorf("worker_locks: release %s: %w", agentID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("worker_locks: release %s rowsAffected: %w", agentID, err)
	}
	if affected == 0 {
		return ErrLockMissing
	}
	return nil
}

// Get reads the current lock row for agentID. Returns ErrLockMissing
// when no row exists. The function is exported so the supervisor loop
// can inspect lock state between Tick iterations without holding a tx.
func Get(ctx context.Context, q Executor, agentID string) (*Lock, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("%w: agent_id required", ErrInvalidInput)
	}
	if q == nil {
		return nil, fmt.Errorf("%w: executor is nil", ErrInvalidInput)
	}
	row := q.QueryRowContext(ctx,
		`SELECT agent_id, worker_id, fencing_token, lease_expires_at, acquired_at
		   FROM worker_locks
		  WHERE agent_id = ?`,
		agentID,
	)
	var lock Lock
	if err := row.Scan(
		&lock.AgentID,
		&lock.WorkerID,
		&lock.FencingToken,
		&lock.LeaseExpiresAt,
		&lock.AcquiredAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrLockMissing
		}
		return nil, fmt.Errorf("worker_locks: get %s: %w", agentID, err)
	}
	return &lock, nil
}

// ---------------------------------------------------------------------------
// Internal helpers — kept out of the spec-facing API surface above.
// ---------------------------------------------------------------------------

// validateAcquireInput is the shared validation block used by Acquire
// and Heartbeat. The two share enough surface that duplicating the
// checks would just drift over time.
func validateAcquireInput(agentID, workerID string, leaseTTL int64, now func() int64) error {
	if strings.TrimSpace(agentID) == "" {
		return fmt.Errorf("%w: agent_id required", ErrInvalidInput)
	}
	if strings.TrimSpace(workerID) == "" {
		return fmt.Errorf("%w: worker_id required", ErrInvalidInput)
	}
	if leaseTTL <= 0 {
		return fmt.Errorf("%w: lease_ttl must be positive, got %d", ErrInvalidInput, leaseTTL)
	}
	if now == nil {
		return fmt.Errorf("%w: now func is nil", ErrInvalidInput)
	}
	return nil
}

// insertLock writes the first-ever row for an agent. The caller has
// already proven (via SELECT inside the IMMEDIATE tx) that no row
// exists; we still rely on the PK constraint to surface a race we
// somehow missed.
func insertLock(
	ctx context.Context,
	exec Executor,
	agentID, workerID string,
	fencingToken, now, leaseTTL int64,
) (*Lock, error) {
	_, err := exec.ExecContext(ctx,
		`INSERT INTO worker_locks
		   (agent_id, worker_id, fencing_token, lease_expires_at, acquired_at)
		 VALUES (?, ?, ?, ?, ?)`,
		agentID, workerID, fencingToken, now+leaseTTL, now,
	)
	if err != nil {
		return nil, fmt.Errorf("worker_locks: insert %s: %w", agentID, err)
	}
	return &Lock{
		AgentID:        agentID,
		WorkerID:       workerID,
		FencingToken:   fencingToken,
		LeaseExpiresAt: now + leaseTTL,
		AcquiredAt:     now,
	}, nil
}

// stealLock executes the §1.4.9 steal UPDATE inside the IMMEDIATE tx.
// The CAS predicate `fencing_token = :prev` defends against a stranger
// stealing twice in a row even within the same IMMEDIATE block.
func stealLock(
	ctx context.Context,
	exec Executor,
	agentID, workerID string,
	prevToken, now, leaseTTL int64,
) (*Lock, error) {
	newToken := prevToken + 1
	res, err := exec.ExecContext(ctx,
		`UPDATE worker_locks
		    SET worker_id = ?,
		        fencing_token = ?,
		        lease_expires_at = ?,
		        acquired_at = ?
		  WHERE agent_id = ?
		    AND fencing_token = ?
		    AND lease_expires_at <= ?`,
		workerID, newToken, now+leaseTTL, now, agentID, prevToken, now,
	)
	if err != nil {
		return nil, fmt.Errorf("worker_locks: steal %s: %w", agentID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("worker_locks: steal %s rowsAffected: %w", agentID, err)
	}
	if affected != 1 {
		// Inside IMMEDIATE this should never happen — but if some
		// future caller relaxes the isolation we surface the race
		// rather than silently dropping the steal.
		return nil, fmt.Errorf("worker_locks: steal %s CAS lost (affected=%d)", agentID, affected)
	}
	return &Lock{
		AgentID:        agentID,
		WorkerID:       workerID,
		FencingToken:   newToken,
		LeaseExpiresAt: now + leaseTTL,
		AcquiredAt:     now,
	}, nil
}
