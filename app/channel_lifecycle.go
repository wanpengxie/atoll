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
	lifecycleTick  = 250 * time.Millisecond
	lifecycleDrain = 16
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
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				w.drain()
			case <-w.wake:
				w.drain()
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
	if !job.Receipt.Valid && !job.Published.Valid {
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
		raw, _ := json.Marshal(receipt)
		if _, err := a.db.ExecContext(ctx, `UPDATE channel_provision_jobs SET receipt_json=?,attempt=attempt+1,last_error=NULL,error_code=NULL WHERE job_id=?`, string(raw), id); err != nil {
			return err
		}
		job.Receipt = sql.NullString{String: string(raw), Valid: true}
	}
	if !job.Published.Valid {
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
			return a.retryProvision(ctx, id, "storage_unavailable", err)
		}
		now := time.Now().UnixMilli()
		if _, err := tx.ExecContext(ctx, `UPDATE channel_provision_jobs SET published_at=? WHERE job_id=?`, now, id); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
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
	if err := a.finalizeProvision(ctx, job, spec, bundle); err != nil {
		var integrity finalizeIntegrityError
		if errors.As(err, &integrity) {
			return a.deadProvision(ctx, id, "invalid_job", err)
		}
		return a.retryProvision(ctx, id, "open_failed", err)
	}
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

type finalizeIntegrityError struct{ error }

func (a *App) finalizeProvision(ctx context.Context, job provisionJob, spec channelhost.ProvisionSpec, bundle channelhost.Bundle) error {
	for _, genesis := range spec.GenesisDeclarations {
		action, payload, acked, found, err := a.loadFinalizeDelivery(ctx, job.OperationID, genesis.DeclID)
		if err != nil {
			return err
		}
		if !found {
			action, payload, found, err = a.createFinalizeDelivery(ctx, job, genesis)
			if err != nil {
				return err
			}
		}
		if !found || acked {
			continue
		}
		if err := a.admission.deliverFinalize(ctx, bundle, action, payload); err != nil {
			var operationErr *channel.OperationError
			if errors.As(err, &operationErr) && !operationErr.Retryable {
				_, writeErr := a.db.ExecContext(ctx, `UPDATE channel_finalize_deliveries SET acked_at=?,error_code=? WHERE operation_id=? AND decl_id=?`, time.Now().UnixMilli(), string(operationErr.Code), job.OperationID, genesis.DeclID)
				return finalizeIntegrityError{errors.Join(err, writeErr)}
			}
			return err
		}
		if _, err := a.db.ExecContext(ctx, `UPDATE channel_finalize_deliveries SET acked_at=?,error_code=NULL WHERE operation_id=? AND decl_id=?`, time.Now().UnixMilli(), job.OperationID, genesis.DeclID); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) loadFinalizeDelivery(ctx context.Context, operationID, declID string) (action string, payload json.RawMessage, acked bool, found bool, err error) {
	var raw string
	var ack sql.NullInt64
	err = a.db.QueryRowContext(ctx, `SELECT action,payload_json,acked_at FROM channel_finalize_deliveries WHERE operation_id=? AND decl_id=?`, operationID, declID).Scan(&action, &raw, &ack)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, false, false, nil
	}
	if err != nil {
		return "", nil, false, false, err
	}
	return action, json.RawMessage(raw), ack.Valid, true, nil
}

func (a *App) createFinalizeDelivery(ctx context.Context, job provisionJob, genesis channelhost.GenesisDeclaration) (string, json.RawMessage, bool, error) {
	facts, err := (compositionResolver{app: a}).ResolveDeclaration(ctx, channel.ID(job.ChannelID), genesis.DeclID)
	action := "apply"
	if errors.Is(err, channel.ErrDeclarationNotFound) {
		if strings.HasPrefix(genesis.DeclID, "sys:") {
			return "", nil, false, nil
		}
		action = "revoke"
	} else if err != nil {
		return "", nil, false, err
	} else {
		facts.Rendered.Placement = genesis.Rendered.Placement
		facts.Rendered, err = facts.Rendered.Seal()
		if err != nil {
			return "", nil, false, err
		}
		if facts.Rendered.Digest == genesis.Rendered.Digest {
			return "", nil, false, nil
		}
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return "", nil, false, err
	}
	defer tx.Rollback()
	ref := channel.DerivedFinalizeRef(job.OperationID, genesis.DeclID)
	var request any
	var renderSeq any
	if action == "revoke" {
		request = channel.RevokeDeclRequest{Ref: ref, DeclID: genesis.DeclID}
	} else {
		// Realm declarations do not own channel placement. Finalize re-renders
		// the value fields while retaining the placement frozen into genesis.
		if _, err := tx.ExecContext(ctx, `INSERT INTO decl_render_state(channel_id,decl_id,render_seq) VALUES (?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET render_seq=MAX(render_seq,excluded.render_seq)`, job.ChannelID, genesis.DeclID, genesis.Rendered.RenderSeq); err != nil {
			return "", nil, false, err
		}
		var seq int64
		if err := tx.QueryRowContext(ctx, `UPDATE decl_render_state SET render_seq=render_seq+1 WHERE channel_id=? AND decl_id=? RETURNING render_seq`, job.ChannelID, genesis.DeclID).Scan(&seq); err != nil {
			return "", nil, false, err
		}
		facts.Rendered.RenderSeq = seq
		facts.Rendered, err = facts.Rendered.Seal()
		if err != nil {
			return "", nil, false, err
		}
		renderSeq = seq
		request = channel.ApplyDeclVersionRequest{Ref: ref, DeclID: genesis.DeclID, Rendered: facts.Rendered, Authority: channel.AuthorityRealm}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return "", nil, false, err
	}
	digest, err := channel.Digest(request)
	if err != nil {
		return "", nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO channel_finalize_deliveries(operation_id,decl_id,action,ref,request_digest,payload_json,render_seq) VALUES (?,?,?,?,?,?,?)`, job.OperationID, genesis.DeclID, action, ref, digest, string(payload), renderSeq); err != nil {
		return "", nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return "", nil, false, err
	}
	action, payload, _, found, err := a.loadFinalizeDelivery(ctx, job.OperationID, genesis.DeclID)
	if err == nil && !found {
		return "", nil, false, finalizeIntegrityError{fmt.Errorf("finalize ref collision for operation %s declaration %s", job.OperationID, genesis.DeclID)}
	}
	return action, payload, found, err
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

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<(attempt-1)) * 250 * time.Millisecond
}
