package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type declFanoutTarget struct {
	ChannelID  string `json:"channel_id"`
	InstanceID string `json:"instance_id"`
}

type daemonFanoutTarget struct {
	ChannelID string `json:"channel_id"`
}

type fanoutJob struct {
	table, key, op, initiator, targets string
	id, attempt                        int64
}

type fanoutWorker struct {
	app    *App
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan struct{}
	once   sync.Once
	cursor [2]int64
	next   int
}

func newFanoutWorker(a *App) *fanoutWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &fanoutWorker{app: a, ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
}

func (w *fanoutWorker) start() { go w.run() }

func (w *fanoutWorker) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *fanoutWorker) close() {
	w.once.Do(w.cancel)
	t := time.NewTimer(10 * time.Second)
	defer t.Stop()
	select {
	case <-w.done:
	case <-t.C:
		w.app.logger.Error("fanout_worker_join_timeout")
	}
}

func (w *fanoutWorker) run() {
	defer close(w.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		w.drain()
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
	}
}

func (w *fanoutWorker) drain() {
	for {
		progress := false
		for n := 0; n < 2; n++ {
			idx := w.next
			w.next = 1 - w.next
			job, ok, err := w.claim(idx)
			if err != nil {
				w.app.logger.Error("fanout.claim_failed", "table", idx, "err", err)
				return
			}
			if !ok {
				continue
			}
			progress = true
			if err := w.apply(job); err != nil {
				w.fail(job, err)
			} else {
				w.complete(job)
			}
			break
		}
		if !progress {
			return
		}
		select {
		case <-w.ctx.Done():
			return
		default:
		}
	}
}

func (w *fanoutWorker) claim(which int) (fanoutJob, bool, error) {
	table := "decl_fanout_jobs"
	keyCol := "decl_id"
	if which == 1 {
		table, keyCol = "daemon_revoke_jobs", "daemon_id"
	}
	conn, err := w.app.db.Conn(w.ctx)
	if err != nil {
		return fanoutJob{}, false, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(w.ctx, `BEGIN IMMEDIATE`); err != nil {
		return fanoutJob{}, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	query := fmt.Sprintf(`SELECT job_id,op,%s,%s,targets_json,attempt FROM %s WHERE done_at IS NULL AND job_id>? ORDER BY job_id LIMIT 1`, keyCol, map[bool]string{true: "initiator", false: "''"}[which == 0], table)
	var job fanoutJob
	job.table = table
	scan := func(cursor int64) error {
		return conn.QueryRowContext(w.ctx, query, cursor).Scan(&job.id, &job.op, &job.key, &job.initiator, &job.targets, &job.attempt)
	}
	err = scan(w.cursor[which])
	if errors.Is(err, sql.ErrNoRows) && w.cursor[which] != 0 {
		w.cursor[which] = 0
		err = scan(0)
	}
	if errors.Is(err, sql.ErrNoRows) {
		_, err = conn.ExecContext(w.ctx, `COMMIT`)
		committed = err == nil
		return fanoutJob{}, false, err
	}
	if err != nil {
		return fanoutJob{}, false, err
	}
	if _, err := conn.ExecContext(w.ctx, fmt.Sprintf(`UPDATE %s SET attempt=attempt+1 WHERE job_id=?`, table), job.id); err != nil {
		return fanoutJob{}, false, err
	}
	if _, err := conn.ExecContext(w.ctx, `COMMIT`); err != nil {
		return fanoutJob{}, false, err
	}
	committed = true
	job.attempt++
	w.cursor[which] = job.id
	return job, true, nil
}

func (w *fanoutWorker) apply(job fanoutJob) error {
	switch job.table {
	case "decl_fanout_jobs":
		var targets []declFanoutTarget
		if err := json.Unmarshal([]byte(job.targets), &targets); err != nil {
			return fmt.Errorf("decode decl targets: %w", err)
		}
		release := w.app.declLocks.lock(job.key)
		defer release()
		for _, target := range targets {
			h := w.app.getHome(channel.ID(target.ChannelID))
			if h == nil {
				if _, exists := w.app.channelWorkspaceID(w.ctx, target.ChannelID); exists {
					return fmt.Errorf("channel %s home unavailable", target.ChannelID)
				}
				continue
			}
			id := actor.ActorID(target.InstanceID)
			if job.op == "delete" {
				if err := h.RemoveInstance(w.ctx, id); err != nil && !errors.Is(err, storespec.ErrCompositionNotFound) {
					return err
				}
			} else {
				_, _, err := h.ApplyRestartTarget(w.ctx, job.id, id)
				if err != nil && !errors.Is(err, storespec.ErrCompositionNotFound) {
					return err
				}
			}
		}
		return nil
	case "daemon_revoke_jobs":
		var targets []daemonFanoutTarget
		if err := json.Unmarshal([]byte(job.targets), &targets); err != nil {
			return fmt.Errorf("decode daemon targets: %w", err)
		}
		release := w.app.daemonLocks.lock(job.key)
		defer release()
		for _, target := range targets {
			if job.op == "detach" {
				var n int
				if err := w.app.db.QueryRowContext(w.ctx, `SELECT COUNT(*) FROM daemon_channels WHERE daemon_id=? AND channel_id=?`, job.key, target.ChannelID).Scan(&n); err != nil {
					return err
				}
				if n != 0 { // re-bound after detach: the queued cleanup is superseded.
					continue
				}
			}
			h := w.app.getHome(channel.ID(target.ChannelID))
			if h == nil {
				if _, exists := w.app.channelWorkspaceID(w.ctx, target.ChannelID); exists {
					return fmt.Errorf("channel %s home unavailable", target.ChannelID)
				}
				continue
			}
			if err := h.RevokeDaemonTarget(w.ctx, job.key); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown fanout table %q", job.table)
	}
}

func (w *fanoutWorker) complete(job fanoutJob) {
	_, err := w.app.db.ExecContext(w.ctx, fmt.Sprintf(`UPDATE %s SET done_at=?,last_error=NULL WHERE job_id=? AND done_at IS NULL`, job.table), time.Now().UnixMilli(), job.id)
	if err != nil && !errors.Is(err, context.Canceled) {
		w.app.logger.Error("fanout.complete_failed", "table", job.table, "job", job.id, "err", err)
	}
}

func (w *fanoutWorker) fail(job fanoutJob, cause error) {
	if errors.Is(cause, context.Canceled) {
		return
	}
	if job.attempt >= 5 {
		_, _ = w.app.db.ExecContext(context.Background(), fmt.Sprintf(`UPDATE %s SET done_at=?,last_error=? WHERE job_id=? AND done_at IS NULL`, job.table), time.Now().UnixMilli(), "poisoned: "+cause.Error(), job.id)
		w.app.logger.Error("fanout.poisoned", "table", job.table, "job", job.id, "err", cause)
		return
	}
	_, _ = w.app.db.ExecContext(context.Background(), fmt.Sprintf(`UPDATE %s SET last_error=? WHERE job_id=? AND done_at IS NULL`, job.table), cause.Error(), job.id)
}
