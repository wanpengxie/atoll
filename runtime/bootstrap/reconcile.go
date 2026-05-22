package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// Reconciler scans bootstrap_registry for rows left in 'in_progress'
// state (a daemon crash mid-saga). Rows that have not reached
// channel_lock are rollback-safe: the workdir is removed and the row is
// marked 'rolled_back'. Rows that already have channel_lock are locally
// complete and must be kept for create-channel ack replay.
//
// The Reconciler also tracks per-channel cold-start held-channel outcomes
// for the daemon — when a `control.held_channels_ack` frame arrives, the
// held-channel ack handler calls
// AcceptHeldChannel / RejectHeldChannel to record the decision. Confirmed
// channels gate downstream zombie-write protection; rejected channels
// trigger the daemon's per-channel unload path.
type Reconciler struct {
	daemonDB *sql.DB
	nowFn    func() int64

	mu           sync.Mutex
	heldAccepted map[channel.ID]int64  // channel_id -> confirmed_at
	heldRejected map[channel.ID]string // channel_id -> reject reason
}

// NewReconciler builds a Reconciler.
func NewReconciler(daemonDB *sql.DB, nowFn func() int64) (*Reconciler, error) {
	if daemonDB == nil {
		return nil, errors.New("bootstrap: NewReconciler daemonDB nil")
	}
	if nowFn == nil {
		return nil, errors.New("bootstrap: NewReconciler nowFn nil")
	}
	return &Reconciler{
		daemonDB:     daemonDB,
		nowFn:        nowFn,
		heldAccepted: make(map[channel.ID]int64),
		heldRejected: make(map[channel.ID]string),
	}, nil
}

// AcceptHeldChannel records that the server accepted ownership of channelID
// at the current wall-clock. Idempotent — repeated calls overwrite the
// timestamp with the most recent. Used by the daemon's
// held-channel ack handler to confirm per-channel ownership after a
// daemonbus reconnect.
func (r *Reconciler) AcceptHeldChannel(channelID channel.ID) {
	if r == nil || channelID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heldAccepted[channelID] = r.nowFn()
	// Clear any prior reject; accept supersedes.
	delete(r.heldRejected, channelID)
}

// RejectHeldChannel records that the server rejected our held-channel report
// for channelID. The daemon's held-channel ack handler reads this map to
// drive the per-channel unload path (zombie writes must not survive a
// ownership loss).
func (r *Reconciler) RejectHeldChannel(channelID channel.ID, reason string) {
	if r == nil || channelID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heldRejected[channelID] = reason
	// Clear any prior accept; reject supersedes — daemon will unload.
	delete(r.heldAccepted, channelID)
}

// HeldChannelAcceptedAt returns the wall-clock at which channelID's
// held-channel report was last accepted, or 0 if it has not been accepted
// (or has since been rejected).
func (r *Reconciler) HeldChannelAcceptedAt(channelID channel.ID) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heldAccepted[channelID]
}

// HeldChannelRejectedReason returns the last recorded rejection reason for
// channelID, or "" if no rejection is currently active.
func (r *Reconciler) HeldChannelRejectedReason(channelID channel.ID) (string, bool) {
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reason, ok := r.heldRejected[channelID]
	return reason, ok
}

// RolledBack records one rollback outcome.
type RolledBack struct {
	CreateRequestID string
	ChannelID       channel.ID
	WorkdirPath     string
}

// Run reconciles every 'in_progress' row. Returns the rows it rolled back.
func (r *Reconciler) Run(ctx context.Context) ([]RolledBack, error) {
	const sel = `SELECT create_request_id, channel_id, workdir_path
	             FROM bootstrap_registry WHERE status='in_progress'`
	rows, err := r.daemonDB.QueryContext(ctx, sel)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: reconcile select: %w", err)
	}
	var staging []RolledBack
	for rows.Next() {
		var rb RolledBack
		var cid string
		if err := rows.Scan(&rb.CreateRequestID, &cid, &rb.WorkdirPath); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("bootstrap: reconcile scan: %w", err)
		}
		rb.ChannelID = channel.ID(cid)
		staging = append(staging, rb)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	completedOrphans, err := r.completedWithoutLock(ctx)
	if err != nil {
		return nil, err
	}
	staging = append(staging, completedOrphans...)

	var rollback []RolledBack
	for _, rb := range staging {
		hasLock, err := channelLockExists(ctx, rb.WorkdirPath)
		if err != nil {
			return nil, err
		}
		if hasLock {
			if err := r.markCompleted(ctx, rb.CreateRequestID); err != nil {
				return nil, err
			}
			continue
		}
		rollback = append(rollback, rb)
	}

	for _, rb := range rollback {
		if rb.WorkdirPath != "" {
			if err := os.RemoveAll(rb.WorkdirPath); err != nil {
				return nil, fmt.Errorf("bootstrap: reconcile rmdir %s: %w", rb.WorkdirPath, err)
			}
		}
		const upd = `UPDATE bootstrap_registry
		             SET status='rolled_back', rollback_reason='crash recovered', completed_at=?
		             WHERE create_request_id=?`
		if _, err := r.daemonDB.ExecContext(ctx, upd, r.nowFn(), rb.CreateRequestID); err != nil {
			return nil, fmt.Errorf("bootstrap: reconcile update: %w", err)
		}
	}
	return rollback, nil
}

func (r *Reconciler) markCompleted(ctx context.Context, createRequestID string) error {
	const upd = `UPDATE bootstrap_registry
	             SET status='completed', completed_at=?
	             WHERE create_request_id=? AND status='in_progress'`
	if _, err := r.daemonDB.ExecContext(ctx, upd, r.nowFn(), createRequestID); err != nil {
		return fmt.Errorf("bootstrap: reconcile mark completed: %w", err)
	}
	return nil
}

func (r *Reconciler) completedWithoutLock(ctx context.Context) ([]RolledBack, error) {
	const sel = `SELECT create_request_id, channel_id, workdir_path
	             FROM bootstrap_registry WHERE status='completed'`
	rows, err := r.daemonDB.QueryContext(ctx, sel)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: reconcile completed select: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []RolledBack
	for rows.Next() {
		var rb RolledBack
		var cid string
		if err := rows.Scan(&rb.CreateRequestID, &cid, &rb.WorkdirPath); err != nil {
			return nil, fmt.Errorf("bootstrap: reconcile completed scan: %w", err)
		}
		rb.ChannelID = channel.ID(cid)
		hasLock, err := channelLockExists(ctx, rb.WorkdirPath)
		if err != nil {
			return nil, err
		}
		if !hasLock {
			out = append(out, rb)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func channelLockExists(ctx context.Context, workdir string) (bool, error) {
	if workdir == "" {
		return false, nil
	}
	sqlitePath := filepath.Join(workdir, "channel.sqlite")
	if _, err := os.Stat(sqlitePath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("bootstrap: reconcile stat channel sqlite %s: %w", sqlitePath, err)
	}
	db, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		if strings.Contains(err.Error(), "file is not a database") {
			return false, nil
		}
		return false, fmt.Errorf("bootstrap: reconcile open channel sqlite %s: %w", sqlitePath, err)
	}
	defer func() { _ = db.Close() }()
	lock := store.NewChannelLock(db)
	_, ok, err := lock.Get(ctx)
	if err != nil {
		return false, fmt.Errorf("bootstrap: reconcile read channel_lock %s: %w", sqlitePath, err)
	}
	return ok, nil
}
