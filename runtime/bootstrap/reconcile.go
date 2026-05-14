package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// Reconciler scans bootstrap_registry for rows left in 'in_progress'
// state (a daemon crash mid-saga). Each such row is rolled back: the
// workdir is removed and the row is marked 'rolled_back'.
type Reconciler struct {
	daemonDB *sql.DB
	nowFn    func() int64
}

// NewReconciler builds a Reconciler.
func NewReconciler(daemonDB *sql.DB, nowFn func() int64) (*Reconciler, error) {
	if daemonDB == nil {
		return nil, errors.New("bootstrap: NewReconciler daemonDB nil")
	}
	if nowFn == nil {
		return nil, errors.New("bootstrap: NewReconciler nowFn nil")
	}
	return &Reconciler{daemonDB: daemonDB, nowFn: nowFn}, nil
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

	for _, rb := range staging {
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
	return staging, nil
}
