package harness

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/coagent-ai/daemon-go/internal/supervisor"
)

// SQLiteWorkerLocks adapts a channel-local *sql.DB into a
// pkg/harness.WorkerLockLookup. It calls into internal/supervisor's
// Get helper and compares fencing_token + lease.
type SQLiteWorkerLocks struct {
	db *sql.DB
	// Now lets tests inject a deterministic clock. Production leaves
	// this nil → use time.Now().Unix().
	Now func() int64
}

// NewSQLiteWorkerLocks wraps db with the production wall-clock.
func NewSQLiteWorkerLocks(db *sql.DB) *SQLiteWorkerLocks {
	return &SQLiteWorkerLocks{db: db}
}

// IsActive reports whether (agentID, fencingToken) currently matches an
// unexpired worker_locks row. Returns false on missing row, mismatched
// fencing_token, or expired lease (matches L2 §1.4.9 spawn protocol).
func (w *SQLiteWorkerLocks) IsActive(ctx context.Context, agentID string, fencingToken int64) (bool, error) {
	lock, err := supervisor.Get(ctx, w.db, agentID)
	if err != nil {
		if errors.Is(err, supervisor.ErrLockMissing) {
			return false, nil
		}
		return false, fmt.Errorf("sqlite_worker_locks: get %q: %w", agentID, err)
	}
	if lock.FencingToken != fencingToken {
		return false, nil
	}
	now := time.Now().Unix()
	if w.Now != nil {
		now = w.Now()
	}
	if lock.Expired(now) {
		return false, nil
	}
	return true, nil
}
