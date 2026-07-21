package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const fanoutDrain = 32

type fanoutJob struct {
	table, key, op, initiator, baseRef string
	id, attempt                        int64
}

type permanentFanoutError struct{ cause error }

func (e permanentFanoutError) Error() string { return e.cause.Error() }
func (e permanentFanoutError) Unwrap() error { return e.cause }
func permanentFanout(cause error) error      { return permanentFanoutError{cause: cause} }

func fanoutRetryDelay(attempt int64) time.Duration {
	switch attempt {
	case 1:
		return 250 * time.Millisecond
	case 2:
		return time.Second
	case 3:
		return 4 * time.Second
	case 4:
		return 16 * time.Second
	default:
		return time.Minute
	}
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
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
			w.drain()
		case <-ticker.C:
			w.drain()
		}
	}
}

func (w *fanoutWorker) drain() {
	for processed := 0; processed < fanoutDrain; processed++ {
		var job fanoutJob
		found := false
		for range 2 {
			which := w.next
			w.next = 1 - w.next
			candidate, ok, err := w.claim(which)
			if err != nil {
				w.app.logger.Error("fanout.claim_failed", "queue", which, "err", err)
				return
			}
			if ok {
				job, found = candidate, true
				break
			}
		}
		if !found {
			return
		}
		if err := w.apply(job); err != nil {
			w.fail(job, err)
		} else {
			w.complete(job)
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
	query := `SELECT job_id,base_ref,op,decl_id,initiator,attempt FROM decl_fanout_jobs
		WHERE done_at IS NULL AND dead_at IS NULL AND next_attempt_at<=? AND job_id>?
		ORDER BY next_attempt_at,job_id LIMIT 1`
	if which == 1 {
		table = "daemon_revoke_jobs"
		query = `SELECT job_id,base_ref,'revoke',daemon_id,initiator,attempt FROM daemon_revoke_jobs
			WHERE done_at IS NULL AND dead_at IS NULL AND next_attempt_at<=? AND job_id>?
			ORDER BY next_attempt_at,job_id LIMIT 1`
	}
	now := time.Now().UnixMilli()
	var job fanoutJob
	job.table = table
	scan := func(cursor int64) error {
		return w.app.db.QueryRowContext(w.ctx, query, now, cursor).Scan(&job.id, &job.baseRef, &job.op, &job.key, &job.initiator, &job.attempt)
	}
	err := scan(w.cursor[which])
	if errors.Is(err, sql.ErrNoRows) && w.cursor[which] != 0 {
		w.cursor[which] = 0
		err = scan(0)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutJob{}, false, nil
	}
	if err != nil {
		return fanoutJob{}, false, err
	}
	if _, err := w.app.db.ExecContext(w.ctx, `UPDATE `+table+` SET attempt=attempt+1 WHERE job_id=?`, job.id); err != nil {
		return fanoutJob{}, false, err
	}
	job.attempt++
	w.cursor[which] = job.id
	return job, true, nil
}

func (w *fanoutWorker) apply(job fanoutJob) error {
	if job.table == "decl_fanout_jobs" && job.op != "delete" && job.op != "restart" {
		return permanentFanout(fmt.Errorf("unknown declaration fanout op %q", job.op))
	}
	if job.table != "decl_fanout_jobs" && job.table != "daemon_revoke_jobs" {
		return permanentFanout(fmt.Errorf("unknown fanout table %q", job.table))
	}
	channelIDs, err := w.app.directoryChannelIDs(w.ctx)
	if err != nil {
		return err
	}
	var transient []error
	var permanent []error
	for _, chID := range channelIDs {
		err := w.deliverToChannel(job, chID)
		if err != nil {
			var operationErr *channel.OperationError
			if errors.As(err, &operationErr) && !operationErr.Retryable {
				permanent = append(permanent, fmt.Errorf("channel %s: %w", chID, err))
				continue
			}
			transient = append(transient, fmt.Errorf("channel %s: %w", chID, err))
		}
	}
	if len(permanent) > 0 {
		return permanentFanout(errors.Join(permanent...))
	}
	if len(transient) > 0 {
		return errors.Join(transient...)
	}
	return nil
}

// deliverToChannel delivers one fanout job to one channel inside that channel's
// critical section — the same per-channel lock admission delivery holds. This
// is the serialization the delivery-currency rule rests on: an attach's
// present-state daemon check and its membrane commit can never interleave with
// a revoke arm's sweep of the same channel, so either the binding exists when
// revocation sweeps, or the registry row is already gone when attach re-checks.
func (w *fanoutWorker) deliverToChannel(job fanoutJob, chID channel.ID) error {
	release := w.app.channelLocks.lock(string(chID))
	defer release()
	bundle, ok := w.app.host.Acquire(chID)
	if !ok {
		return fmt.Errorf("channel unavailable")
	}
	ref := channel.DerivedFanoutRef(job.baseRef, chID)
	switch job.table {
	case "decl_fanout_jobs":
		switch job.op {
		case "delete":
			_, err := bundle.SysOp().RevokeDeclTargets(w.ctx, channel.RevokeDeclRequest{Ref: ref, DeclID: job.key})
			return err
		case "restart":
			return w.applyVersionDelivery(job, chID, bundle)
		default:
			return &channel.OperationError{Code: channel.ErrCodeInternal, Detail: fmt.Sprintf("unknown declaration fanout op %q", job.op)}
		}
	case "daemon_revoke_jobs":
		_, err := bundle.SysOp().RevokeDaemon(w.ctx, channel.DaemonRequest{Ref: ref, DaemonID: job.key})
		return err
	default:
		return &channel.OperationError{Code: channel.ErrCodeInternal, Detail: fmt.Sprintf("unknown fanout table %q", job.table)}
	}
}

func (a *App) directoryChannelIDs(ctx context.Context) ([]channel.ID, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT id FROM channels ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []channel.ID
	for rows.Next() {
		var id channel.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (w *fanoutWorker) applyVersionDelivery(job fanoutJob, chID channel.ID, bundle channelhost.Bundle) error {
	request, found, err := w.loadFanoutDelivery(job.id, chID)
	if err != nil {
		return err
	}
	if !found {
		request, found, err = w.createFanoutDelivery(job, chID, bundle)
		if err != nil || !found {
			return err
		}
	}
	_, err = bundle.SysOp().ApplyDeclVersion(w.ctx, request)
	if err != nil {
		var operationErr *channel.OperationError
		if errors.As(err, &operationErr) && operationErr.Code == channel.ErrCodeRefConflict {
			_, writeErr := w.app.db.ExecContext(w.ctx, `UPDATE decl_fanout_deliveries SET acked_at=?,error_code=? WHERE job_id=? AND channel_id=?`, time.Now().UnixMilli(), string(operationErr.Code), job.id, string(chID))
			w.app.logger.Error("fanout.ref_conflict", "job", job.id, "channel", chID, "err", err)
			return writeErr
		}
		return err
	}
	_, err = w.app.db.ExecContext(w.ctx, `UPDATE decl_fanout_deliveries SET acked_at=?,error_code=NULL WHERE job_id=? AND channel_id=?`, time.Now().UnixMilli(), job.id, string(chID))
	return err
}

func (w *fanoutWorker) loadFanoutDelivery(jobID int64, chID channel.ID) (channel.ApplyDeclVersionRequest, bool, error) {
	var raw string
	err := w.app.db.QueryRowContext(w.ctx, `SELECT payload_json FROM decl_fanout_deliveries WHERE job_id=? AND channel_id=?`, jobID, string(chID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return channel.ApplyDeclVersionRequest{}, false, nil
	}
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	var request channel.ApplyDeclVersionRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		return channel.ApplyDeclVersionRequest{}, false, permanentFanout(fmt.Errorf("decode delivery: %w", err))
	}
	return request, true, nil
}

func (w *fanoutWorker) createFanoutDelivery(job fanoutJob, chID channel.ID, bundle channelhost.Bundle) (channel.ApplyDeclVersionRequest, bool, error) {
	rows, err := bundle.SysOp().DeclaredBySourceSerialized(w.ctx, job.key)
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if len(rows) == 0 {
		return channel.ApplyDeclVersionRequest{}, false, nil
	}
	current := rows[0]
	tx, err := w.app.db.BeginTx(w.ctx, nil)
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	defer tx.Rollback()
	var class string
	var global sql.NullString
	var deleted sql.NullInt64
	if err := tx.QueryRowContext(w.ctx, `SELECT default_class,config_json,deleted_at FROM actor_decls WHERE id=?`, job.key).Scan(&class, &global, &deleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return channel.ApplyDeclVersionRequest{}, false, nil
		}
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if deleted.Valid {
		return channel.ApplyDeclVersionRequest{}, false, nil
	}
	config := global.String
	var overlay sql.NullString
	err = tx.QueryRowContext(w.ctx, `SELECT config_json FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, string(chID), job.key).Scan(&overlay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if err == nil && overlay.Valid {
		config = overlay.String
	}
	placement := channel.Placement{Kind: channel.PlacementKind(current.Placement.Kind), DesiredHost: current.Placement.Host}
	var rawConfig json.RawMessage
	if config != "" {
		rawConfig = json.RawMessage(config)
	}
	candidate, err := (channel.RenderedSnapshot{Class: class, Config: rawConfig, Placement: placement, TIdleMS: current.TIdle.Milliseconds(), RenderSeq: 1}).Seal()
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	currentSnapshot, err := (channel.RenderedSnapshot{Class: current.Class, Config: current.Config, Placement: placement, TIdleMS: current.TIdle.Milliseconds(), RenderSeq: current.RenderSeq}).Seal()
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if candidate.Digest == currentSnapshot.Digest {
		return channel.ApplyDeclVersionRequest{}, false, nil
	}
	if _, err := tx.ExecContext(w.ctx, `INSERT INTO decl_render_state(channel_id,decl_id,render_seq) VALUES (?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET render_seq=MAX(render_seq,excluded.render_seq)`, string(chID), job.key, current.RenderSeq); err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	var seq int64
	if err := tx.QueryRowContext(w.ctx, `UPDATE decl_render_state SET render_seq=render_seq+1 WHERE channel_id=? AND decl_id=? RETURNING render_seq`, string(chID), job.key).Scan(&seq); err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	candidate.RenderSeq = seq
	request := channel.ApplyDeclVersionRequest{
		Ref: channel.DerivedFanoutRef(job.baseRef, chID), DeclID: job.key,
		Rendered: candidate, Authority: channel.AuthorityRealm,
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if _, err := tx.ExecContext(w.ctx, `INSERT INTO decl_fanout_deliveries(job_id,channel_id,ref,render_seq,digest,payload_json) VALUES (?,?,?,?,?,?)`, job.id, string(chID), request.Ref, seq, candidate.Digest, string(raw)); err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return channel.ApplyDeclVersionRequest{}, false, err
	}
	return request, true, nil
}

func (w *fanoutWorker) complete(job fanoutJob) {
	_, err := w.app.db.ExecContext(w.ctx, `UPDATE `+job.table+` SET done_at=?,last_error=NULL WHERE job_id=? AND done_at IS NULL`, time.Now().UnixMilli(), job.id)
	if err != nil && !errors.Is(err, context.Canceled) {
		w.app.logger.Error("fanout.complete_failed", "table", job.table, "job", job.id, "err", err)
	}
}

func (w *fanoutWorker) fail(job fanoutJob, cause error) {
	if errors.Is(cause, context.Canceled) {
		return
	}
	var permanent permanentFanoutError
	if errors.As(cause, &permanent) {
		_, _ = w.app.db.ExecContext(context.Background(), `UPDATE `+job.table+` SET dead_at=?,last_error=?,next_attempt_at=0 WHERE job_id=? AND done_at IS NULL AND dead_at IS NULL`, time.Now().UnixMilli(), permanent.Error(), job.id)
		w.app.logger.Error("fanout.dead_letter", "table", job.table, "job", job.id, "err", permanent)
		return
	}
	next := time.Now().Add(fanoutRetryDelay(job.attempt)).UnixMilli()
	_, _ = w.app.db.ExecContext(context.Background(), `UPDATE `+job.table+` SET last_error=?,next_attempt_at=? WHERE job_id=? AND done_at IS NULL AND dead_at IS NULL`, cause.Error(), next, job.id)
}
