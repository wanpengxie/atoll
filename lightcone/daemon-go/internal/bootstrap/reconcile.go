package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// Reconcile is the daemon-startup pass that walks every
// `bootstrap_registry` row left in status='in_progress' and pushes it
// to a terminal state (L2 §1.4.7 reconcile path):
//
//   - workdir or messages.sqlite missing → DELETE workdir +
//     UPDATE status='rolled_back', rollback_reason='workdir_incomplete'.
//   - workdir + sqlite present → retry step 8a (INSERT OR IGNORE
//     channel_created event) then step 8b (CAS UPDATE completed).
//
// INSERT OR IGNORE on the deterministic envelope.id
// (`bootstrap:{create_request_id}`) means an already-emitted event is
// a no-op on second attempt — so a crash between step 8a and step 8b
// recovers without duplicating the channel_created event.
func (s *Saga) Reconcile(ctx context.Context) (ReconcileReport, error) {
	var report ReconcileReport

	rows, err := s.daemonDB.QueryContext(ctx,
		`SELECT create_request_id, channel_id, workdir_path
		   FROM bootstrap_registry
		  WHERE status = ?`,
		StatusInProgress,
	)
	if err != nil {
		return report, fmt.Errorf("bootstrap: reconcile scan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type job struct {
		createRequestID string
		channelID       string
		workdirPath     string
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.createRequestID, &j.channelID, &j.workdirPath); err != nil {
			return report, fmt.Errorf("bootstrap: reconcile row scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return report, fmt.Errorf("bootstrap: reconcile rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return report, fmt.Errorf("bootstrap: reconcile close: %w", err)
	}

	for _, j := range jobs {
		report.Scanned++
		channelDBPath := filepath.Join(j.workdirPath, channelDBFilename)

		// Integrity check: both the workdir AND the channel sqlite file
		// must exist on disk to retry step 8-9. Otherwise the bootstrap
		// crashed somewhere in step 2 and there is nothing to recover.
		if !s.statExists(j.workdirPath) || !s.statExists(channelDBPath) {
			s.compensate(ctx, j.createRequestID, j.workdirPath,
				fmt.Errorf("workdir_incomplete: workdir=%q channelDB=%q",
					j.workdirPath, channelDBPath))
			report.RolledBack++
			continue
		}

		// Channel sqlite is present — retry step 8a + 8b.
		channelDB, err := s.openCh(ctx, channelDBPath)
		if err != nil {
			report.Failures = append(report.Failures,
				fmt.Sprintf("%s: open channel: %v", j.createRequestID, err))
			continue
		}
		err = s.retryStep8(ctx, channelDB, j.createRequestID, j.channelID)
		_ = channelDB.Close()
		if err != nil {
			report.Failures = append(report.Failures,
				fmt.Sprintf("%s: retry step8: %v", j.createRequestID, err))
			continue
		}
		report.Completed++
	}

	return report, nil
}

// retryStep8 re-runs step 8a (INSERT OR IGNORE channel_created event)
// and step 8b (CAS UPDATE bootstrap_registry status=completed) on an
// already-open channel sqlite. The IMMEDIATE tx ensures step 8a is
// committed before we CAS the registry.
func (s *Saga) retryStep8(ctx context.Context, channelDB *sql.DB, createRequestID, channelID string) error {
	now := s.now()
	err := store.WithImmediate(ctx, channelDB, func(ctx context.Context, conn *sql.Conn) error {
		return emitChannelCreated(ctx, conn, CreateParams{
			CreateRequestID: createRequestID,
			ChannelID:       channelID,
		}, now)
	})
	if err != nil {
		return fmt.Errorf("step8a retry: %w", err)
	}
	res, err := s.daemonDB.ExecContext(ctx,
		`UPDATE bootstrap_registry
		   SET status=?, completed_at=?
		 WHERE create_request_id=? AND status=?`,
		StatusCompleted, now, createRequestID, StatusInProgress,
	)
	if err != nil {
		return fmt.Errorf("step8b retry: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return fmt.Errorf("step8b retry CAS lost (affected=%d)", affected)
	}
	return nil
}

// ListChannels returns every completed bootstrap row, ordered by
// completed_at ascending. Used by the server reconcile API
// (daemon:list_channels) to rebuild its informational cache.
func (s *Saga) ListChannels(ctx context.Context) ([]ChannelInfo, error) {
	rows, err := s.daemonDB.QueryContext(ctx,
		`SELECT create_request_id, channel_id, workdir_path,
		        COALESCE(completed_at, 0) AS completed_at
		   FROM bootstrap_registry
		  WHERE status = ?
		  ORDER BY completed_at ASC, channel_id ASC`,
		StatusCompleted,
	)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: list channels: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ChannelInfo
	for rows.Next() {
		var c ChannelInfo
		if err := rows.Scan(&c.CreateRequestID, &c.ChannelID, &c.WorkdirPath, &c.CompletedAt); err != nil {
			return nil, fmt.Errorf("bootstrap: list channels row: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("bootstrap: list channels rows: %w", err)
	}
	return out, nil
}
