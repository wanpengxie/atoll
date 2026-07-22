package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	lifecycleTick            = 250 * time.Millisecond
	lifecycleDrain           = 16
	servingReconcileInterval = 30 * time.Second
	// membershipSweepInterval paces the projection's third maintenance layer;
	// App.New runs the boot pass before the first tick fires.
	membershipSweepInterval = 5 * time.Minute
)

type lifecycleWorker struct {
	app    *App
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
	runMu  sync.Mutex
	// The two cursors are process-local scheduling hints, not claims. Job truth
	// remains entirely in SQLite, so a restart safely begins each ring at zero.
	nextKind      string
	provisionLast int64
	destroyLast   int64
}

func newLifecycleWorker(app *App) *lifecycleWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &lifecycleWorker{
		app: app, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}),
		nextKind: "provision",
	}
}

func (w *lifecycleWorker) start() {
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(lifecycleTick)
		defer ticker.Stop()
		servingTicker := time.NewTicker(servingReconcileInterval)
		defer servingTicker.Stop()
		lastSweep := time.Now()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				w.drain()
				if time.Since(lastSweep) >= membershipSweepInterval {
					lastSweep = time.Now()
					w.app.sweepMembershipProjection(w.ctx)
				}
			case <-w.wake:
				w.drain()
			case <-servingTicker.C:
				if err := w.app.reconcileServingChannels(w.ctx); err != nil {
					w.app.logger.Warn("channel serving reconcile failed", "err", err)
				}
			}
		}
	}()
}

func (w *lifecycleWorker) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *lifecycleWorker) close() { w.once.Do(func() { w.cancel(); <-w.done }) }

func (w *lifecycleWorker) drain() {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	for i := 0; i < lifecycleDrain; i++ {
		kind, id, ok := w.next()
		if !ok {
			return
		}
		if kind == "provision" {
			_ = w.app.runProvisionJob(w.ctx, id)
		} else {
			_ = w.app.runDestroyJob(w.ctx, id)
		}
	}
}

func (w *lifecycleWorker) next() (string, int64, bool) {
	first, second := w.nextKind, "destroy"
	if first == "destroy" {
		second = "provision"
	}
	for _, kind := range []string{first, second} {
		id, ok := w.nextOfKind(kind)
		if !ok {
			continue
		}
		if kind == "provision" {
			w.provisionLast = id
			w.nextKind = "destroy"
		} else {
			w.destroyLast = id
			w.nextKind = "provision"
		}
		return kind, id, true
	}
	return "", 0, false
}

func (w *lifecycleWorker) nextOfKind(kind string) (int64, bool) {
	now := time.Now().UnixMilli()
	last := w.destroyLast
	table := "channel_destroy_jobs"
	extra := ""
	if kind == "provision" {
		last = w.provisionLast
		table = "channel_provision_jobs"
		extra = " AND compensation_job_id IS NULL"
	}
	query := `SELECT job_id FROM ` + table + ` WHERE done_at IS NULL AND dead_at IS NULL` + extra +
		` AND next_attempt_at<=? AND job_id>? ORDER BY next_attempt_at,job_id LIMIT 1`
	var id int64
	if err := w.app.db.QueryRowContext(w.ctx, query, now, last).Scan(&id); err == nil {
		return id, true
	}
	// Wrap the ring. The compound pending index gives the same deterministic
	// order as the claim query on both the forward and wrapped legs.
	query = `SELECT job_id FROM ` + table + ` WHERE done_at IS NULL AND dead_at IS NULL` + extra +
		` AND next_attempt_at<=? ORDER BY next_attempt_at,job_id LIMIT 1`
	if err := w.app.db.QueryRowContext(w.ctx, query, now).Scan(&id); err != nil {
		return 0, false
	}
	return id, true
}

type provisionJob struct {
	ID                                                               int64
	OperationID, ChannelID, RequestedBy, Name, Type, Owner, SpecJSON string
	Receipt                                                          sql.NullString
	Published                                                        sql.NullInt64
	Compensation                                                     sql.NullInt64
	Attempt                                                          int
	Done, Dead                                                       sql.NullInt64
}

func (a *App) loadProvisionJob(ctx context.Context, id int64) (provisionJob, error) {
	var job provisionJob
	err := a.db.QueryRowContext(ctx, `SELECT job_id,operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,
		receipt_json,published_at,compensation_job_id,attempt,done_at,dead_at FROM channel_provision_jobs WHERE job_id=?`, id).
		Scan(&job.ID, &job.OperationID, &job.ChannelID, &job.RequestedBy, &job.Name, &job.Type, &job.Owner, &job.SpecJSON,
			&job.Receipt, &job.Published, &job.Compensation, &job.Attempt, &job.Done, &job.Dead)
	return job, err
}

func (a *App) runProvisionJob(ctx context.Context, id int64) error {
	job, err := a.loadProvisionJob(ctx, id)
	if err != nil || job.Done.Valid || job.Dead.Valid {
		return err
	}
	release := a.channelLocks.lock(job.ChannelID)
	defer release()
	job, err = a.loadProvisionJob(ctx, id)
	if err != nil || job.Done.Valid || job.Dead.Valid {
		return err
	}
	if job.Compensation.Valid {
		if err := a.runDestroyJobLocked(ctx, job.Compensation.Int64); err != nil {
			return err
		}
		var done sql.NullInt64
		if err := a.db.QueryRowContext(ctx, `SELECT done_at FROM channel_destroy_jobs WHERE job_id=?`, job.Compensation.Int64).Scan(&done); err != nil || !done.Valid {
			return err
		}
		_, err := a.db.ExecContext(ctx, `UPDATE channel_provision_jobs SET attempt=attempt+1,error_code='name_conflict',last_error='publish name conflict compensated',dead_at=? WHERE job_id=?`, time.Now().UnixMilli(), id)
		return err
	}
	var spec channelhost.ProvisionSpec
	if err := json.Unmarshal([]byte(job.SpecJSON), &spec); err != nil || spec.ChannelID == "" {
		return a.deadProvision(ctx, id, "invalid_job", fmt.Errorf("decode provision spec: %w", err))
	}
	if !job.Published.Valid {
		receipt, err := a.host.Provision(ctx, spec)
		if err != nil {
			if errors.Is(err, channelhost.ErrInvalidChannelID) || errors.Is(err, channelhost.ErrChannelRetired) {
				code := "invalid_channel_id"
				if errors.Is(err, channelhost.ErrChannelRetired) {
					code = "channel_retired"
				}
				return a.deadProvision(ctx, id, code, err)
			}
			return a.retryProvision(ctx, id, "storage_unavailable", err)
		}
		raw, err := json.Marshal(receipt)
		if err != nil {
			return a.deadProvision(ctx, id, "invalid_job", fmt.Errorf("encode provision receipt: %w", err))
		}
		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var parent any
		if spec.Origin != nil && spec.Origin.ParentChannelID != "" {
			parent = string(spec.Origin.ParentChannelID)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO channels(id,name,type,created_at,parent_id) VALUES (?,?,?,?,?)`, job.ChannelID, job.Name, job.Type, spec.CreatedAt, parent)
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "unique") {
			op := "lc:comp:" + uuid.NewString()
			res, insertErr := tx.ExecContext(ctx, `INSERT INTO channel_destroy_jobs(operation_id,channel_id,requested_by,created_at) VALUES (?,?,?,?)`, op, job.ChannelID, job.RequestedBy, time.Now().UnixMilli())
			if insertErr != nil {
				return insertErr
			}
			compID, _ := res.LastInsertId()
			if _, err := tx.ExecContext(ctx, `UPDATE channel_provision_jobs SET compensation_job_id=?,last_error='publish name conflict' WHERE job_id=?`, compID, id); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			return a.runDestroyJobLocked(ctx, compID)
		}
		if err != nil {
			_ = tx.Rollback()
			return a.retryProvision(ctx, id, "storage_unavailable", err)
		}
		now := time.Now().UnixMilli()
		if _, err := tx.ExecContext(ctx, `UPDATE channel_provision_jobs SET receipt_json=?,published_at=?,attempt=attempt+1,last_error=NULL,error_code=NULL WHERE job_id=?`, string(raw), now, id); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		job.Receipt = sql.NullString{String: string(raw), Valid: true}
		job.Published = sql.NullInt64{Int64: now, Valid: true}
	}
	if err := a.host.Open(ctx, channelhost.OpenSpec{ChannelID: channel.ID(job.ChannelID), ExpectedType: job.Type}); err != nil {
		if errors.Is(err, channelhost.ErrSchemaIncompatible) {
			return a.deadProvision(ctx, id, "schema_incompatible", err)
		}
		if errors.Is(err, channelhost.ErrOwnerInvariant) {
			return a.deadProvision(ctx, id, "owner_invariant", err)
		}
		return a.retryProvision(ctx, id, "open_failed", err)
	}
	bundle, ok := a.host.Acquire(channel.ID(job.ChannelID))
	if !ok {
		return a.retryProvision(ctx, id, "open_failed", errors.New("opened channel unavailable"))
	}
	// A provision attempt is intentionally frozen at its genesis snapshot.
	// TODO(fanout-lite): a declaration may be soft-deleted while a persisted
	// provision job is retrying; the frozen instance is still allowed to be
	// born and thereafter behaves like any other retained snapshot.
	actorID, found, err := bundle.View().ResolvePrincipal(ctx, actor.KindHuman, job.Owner)
	if err != nil || !found {
		return a.retryProvision(ctx, id, "open_failed", errors.New("owner projection unavailable"))
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at) VALUES (?,?,?,?) ON CONFLICT(principal,channel_id) DO UPDATE SET actor_id=excluded.actor_id,updated_at=excluded.updated_at`, job.Owner, job.ChannelID, string(actorID), now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channel_provision_jobs SET done_at=?,last_error=NULL,error_code=NULL WHERE job_id=?`, now, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if a.membershipPoke != nil {
		a.membershipPoke(job.Owner)
	}
	return nil
}

func (a *App) retryProvision(ctx context.Context, id int64, code string, cause error) error {
	var attempt int
	_ = a.db.QueryRowContext(ctx, `SELECT attempt FROM channel_provision_jobs WHERE job_id=?`, id).Scan(&attempt)
	next := time.Now().Add(backoff(attempt + 1)).UnixMilli()
	_, err := a.db.ExecContext(ctx, `UPDATE channel_provision_jobs SET attempt=attempt+1,error_code=?,last_error=?,next_attempt_at=? WHERE job_id=?`, code, cause.Error(), next, id)
	return errors.Join(cause, err)
}

func (a *App) deadProvision(ctx context.Context, id int64, code string, cause error) error {
	_, err := a.db.ExecContext(ctx, `UPDATE channel_provision_jobs SET attempt=attempt+1,error_code=?,last_error=?,dead_at=? WHERE job_id=?`, code, cause.Error(), time.Now().UnixMilli(), id)
	return errors.Join(cause, err)
}

func (a *App) runDestroyJob(ctx context.Context, id int64) error {
	var channelID string
	if err := a.db.QueryRowContext(ctx, `SELECT channel_id FROM channel_destroy_jobs WHERE job_id=?`, id).Scan(&channelID); err != nil {
		return err
	}
	release := a.channelLocks.lock(channelID)
	defer release()
	return a.runDestroyJobLocked(ctx, id)
}

func (a *App) runDestroyJobLocked(ctx context.Context, id int64) error {
	var channelID string
	var attempt int
	var done, dead sql.NullInt64
	if err := a.db.QueryRowContext(ctx, `SELECT channel_id,attempt,done_at,dead_at FROM channel_destroy_jobs WHERE job_id=?`, id).Scan(&channelID, &attempt, &done, &dead); err != nil {
		return err
	}
	if done.Valid || dead.Valid {
		return nil
	}
	if err := a.host.Destroy(ctx, channel.ID(channelID)); err != nil {
		if errors.Is(err, channelhost.ErrInvalidChannelID) {
			return a.deadDestroy(ctx, id, "invalid_channel_id", err)
		}
		next := time.Now().Add(backoff(attempt + 1)).UnixMilli()
		_, writeErr := a.db.ExecContext(ctx, `UPDATE channel_destroy_jobs SET attempt=attempt+1,error_code='destroy_failed',last_error=?,next_attempt_at=? WHERE job_id=?`, err.Error(), next, id)
		return errors.Join(err, writeErr)
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE channel_destroy_jobs SET attempt=attempt+1,error_code=NULL,last_error=NULL,done_at=? WHERE job_id=?`, now, id); err != nil {
		return err
	}
	// A publish-name loser is terminal only after the physical image has left
	// Census. Keeping this transition with the destroy receipt closes the crash
	// window without ever making the provision worker reclaim compensation.
	if _, err := tx.ExecContext(ctx, `UPDATE channel_provision_jobs
		SET attempt=attempt+1,error_code='name_conflict',last_error='publish name conflict compensated',dead_at=?
		WHERE compensation_job_id=? AND done_at IS NULL AND dead_at IS NULL`, now, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (a *App) deadDestroy(ctx context.Context, id int64, code string, cause error) error {
	_, err := a.db.ExecContext(ctx, `UPDATE channel_destroy_jobs SET attempt=attempt+1,error_code=?,last_error=?,dead_at=? WHERE job_id=?`, code, cause.Error(), time.Now().UnixMilli(), id)
	a.logger.Error("channel destroy job permanently failed", "job_id", id, "code", code, "err", cause)
	return errors.Join(cause, err)
}

// reconcileServingChannels is the lifecycle level-reconciliation arm. One corrupt or
// unavailable channel is isolated and remains honestly unavailable; it never
// prevents the realm from starting or the next pass from retrying it. Keeping
// Open beside Provision/Destroy makes channelhost's production lifecycle
// calling surface mechanically closed to this file.
func (a *App) reconcileServingChannels(ctx context.Context) error {
	rows, err := a.db.QueryContext(ctx, `SELECT id,type FROM channels ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw, typ string
		if err := rows.Scan(&raw, &typ); err != nil {
			return err
		}
		id := channel.ID(raw)
		if err := a.host.Open(ctx, channelhost.OpenSpec{ChannelID: id, ExpectedType: typ}); err != nil {
			a.logger.Warn("channel open reconcile failed", "channel", raw, "err", err)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	entries, err := a.host.Census(ctx)
	if err != nil {
		a.logger.Warn("channel census failed", "err", err)
		return nil
	}
	for _, entry := range entries {
		exists, err := a.channelExists(ctx, string(entry.ChannelID))
		if err != nil {
			a.logger.Warn("channel directory check failed", "channel", entry.ChannelID, "err", err)
			continue
		}
		if !exists {
			a.logger.Warn("orphan channel image", "channel", entry.ChannelID, "state", entry.State)
		}
	}
	return nil
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<(attempt-1)) * 250 * time.Millisecond
}
