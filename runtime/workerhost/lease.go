package workerhost

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/fence"
)

// Lease is one acquired worker slot for one channel.
type Lease struct {
	ID           string
	ChannelID    channel.ID
	WorkerID     string
	FencingToken fence.FencingToken
	DaemonEpoch  fence.DaemonEpoch
	AcquiredAt   int64
	ExpiresAt    int64
}

// LeaseTTL is the per-T3-spec lease TTL (5 minutes, NOT heartbeat 30s).
const LeaseTTL = 5 * time.Minute

// LeaseStore abstracts the worker_locks table. The concrete impl wraps
// runtime/store (the daemon owns sqlite write rights). Workers never
// touch this directly.
type LeaseStore struct {
	db *sql.DB
}

// NewLeaseStore returns a LeaseStore bound to a channel sqlite.
func NewLeaseStore(db *sql.DB) *LeaseStore { return &LeaseStore{db: db} }

// Acquire writes a worker_locks row (INSERT OR REPLACE on agent_id).
// Returns (lease, true) on success; (zero, false, nil) when a non-stale
// lease for the same agent_id already exists owned by another worker.
//
// Stale leases (lease_expires_at <= now) are overwritten.
func (s *LeaseStore) Acquire(
	ctx context.Context,
	agentID string,
	workerID string,
	fencing fence.FencingToken,
	daemonEpoch fence.DaemonEpoch,
	now int64,
) (Lease, bool, error) {
	if s.db == nil {
		return Lease{}, false, errors.New("workerhost: LeaseStore db nil")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, false, fmt.Errorf("workerhost: lease begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const sel = `SELECT worker_id, fencing_token, daemon_epoch, lease_expires_at, acquired_at
	             FROM worker_locks WHERE agent_id=?`
	var (
		curWorker          string
		curFencing         string
		curEpoch           int64
		curExpires, curAcq int64
	)
	switch err := tx.QueryRowContext(ctx, sel, agentID).Scan(
		&curWorker, &curFencing, &curEpoch, &curExpires, &curAcq,
	); {
	case err == nil:
		// Existing row — overwrite only if the existing lease is stale
		// OR the caller's daemon_epoch strictly dominates (recover from
		// a daemon restart where the old worker already exited).
		//
		// Ordering uses daemon_epoch — fencing_token is opaque random
		// (proto-foundation §3.6.1) and cannot be compared by magnitude.
		if curExpires > now && curEpoch >= int64(daemonEpoch) {
			return Lease{}, false, tx.Commit()
		}
	case errors.Is(err, sql.ErrNoRows):
		// proceed to insert
	default:
		return Lease{}, false, fmt.Errorf("workerhost: lease select: %w", err)
	}

	const upsert = `INSERT INTO worker_locks
	   (agent_id, worker_id, fencing_token, daemon_epoch, lease_expires_at, acquired_at)
	   VALUES (?, ?, ?, ?, ?, ?)
	   ON CONFLICT(agent_id) DO UPDATE SET
	     worker_id=excluded.worker_id,
	     fencing_token=excluded.fencing_token,
	     daemon_epoch=excluded.daemon_epoch,
	     lease_expires_at=excluded.lease_expires_at,
	     acquired_at=excluded.acquired_at`
	expires := now + LeaseTTL.Milliseconds()
	if _, err := tx.ExecContext(ctx, upsert,
		agentID, workerID, string(fencing), int64(daemonEpoch), expires, now,
	); err != nil {
		return Lease{}, false, fmt.Errorf("workerhost: lease upsert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, false, fmt.Errorf("workerhost: lease commit: %w", err)
	}

	return Lease{
		ID:           agentID, // one lease per agent for launch
		ChannelID:    "",      // set by caller
		WorkerID:     workerID,
		FencingToken: fencing,
		DaemonEpoch:  daemonEpoch,
		AcquiredAt:   now,
		ExpiresAt:    expires,
	}, true, nil
}

// Release deletes the lease row (idempotent).
func (s *LeaseStore) Release(ctx context.Context, agentID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM worker_locks WHERE agent_id=?`, agentID); err != nil {
		return fmt.Errorf("workerhost: lease release: %w", err)
	}
	return nil
}

// SweepExpired deletes leases whose expires_at <= now. Returns the
// number of rows removed.
func (s *LeaseStore) SweepExpired(ctx context.Context, now int64) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM worker_locks WHERE lease_expires_at<=?`, now)
	if err != nil {
		return 0, fmt.Errorf("workerhost: lease sweep: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// InMemoryLeaseTable tracks active leases in daemon process memory in
// addition to the sqlite table. It's a fast index for the IPC fence
// path so we don't hit sqlite on every IPC request.
type InMemoryLeaseTable struct {
	mu     sync.RWMutex
	active map[string]Lease // keyed by lease.ID
}

// NewInMemoryLeaseTable returns an empty table.
func NewInMemoryLeaseTable() *InMemoryLeaseTable {
	return &InMemoryLeaseTable{active: make(map[string]Lease)}
}

// Put inserts/updates a lease.
func (t *InMemoryLeaseTable) Put(l Lease) {
	t.mu.Lock()
	t.active[l.ID] = l
	t.mu.Unlock()
}

// Get returns the lease by id.
func (t *InMemoryLeaseTable) Get(id string) (Lease, bool) {
	t.mu.RLock()
	l, ok := t.active[id]
	t.mu.RUnlock()
	return l, ok
}

// Delete removes a lease.
func (t *InMemoryLeaseTable) Delete(id string) {
	t.mu.Lock()
	delete(t.active, id)
	t.mu.Unlock()
}

// List returns a snapshot.
func (t *InMemoryLeaseTable) List() []Lease {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Lease, 0, len(t.active))
	for _, l := range t.active {
		out = append(out, l)
	}
	return out
}
