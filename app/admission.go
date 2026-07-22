package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	admissionDrain      = 16
	admissionSyncWindow = 2 * time.Second
)

var errIdempotencyConflict = errors.New("admission: idempotency key reused with different request")

type admissionCommand struct {
	ChannelID      channel.ID
	Op             string
	Owner          admissionOwner
	OperationID    string
	IdempotencyKey string
	Intent         any
	BuildRequest   func(string) any
}

type admissionOwner struct {
	Principal string
	ActorID   actor.ActorID
}

func principalAdmissionOwner(principal string) admissionOwner {
	return admissionOwner{Principal: principal}
}

func actorAdmissionOwner(id actor.ActorID) admissionOwner { return admissionOwner{ActorID: id} }

func (o admissionOwner) valid() bool { return (o.Principal != "") != (o.ActorID != "") }

func (o admissionOwner) matches(principal, actorID string) bool {
	return o.Principal == principal && string(o.ActorID) == actorID
}

func (o admissionOwner) values() (any, any) {
	var principal, actorID any
	if o.Principal != "" {
		principal = o.Principal
	}
	if o.ActorID != "" {
		actorID = string(o.ActorID)
	}
	return principal, actorID
}

type admissionRecord struct {
	OperationID, ChannelID, Op, RequestedByPrincipal, RequestedByActorID string
	RequestJSON, RequestDigest, Status                                   string
	ResultJSON, ErrorCode                                                sql.NullString
	CreatedAt                                                            int64
	DoneAt                                                               sql.NullInt64
}

type admissionService struct {
	app    *App
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
	runMu  sync.Mutex
	cursor int64
}

func newAdmissionService(app *App) *admissionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &admissionService{app: app, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func (s *admissionService) start() {
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.wake:
				s.drain()
			case <-ticker.C:
				s.drain()
			}
		}
	}()
}

func (s *admissionService) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *admissionService) close() { s.once.Do(func() { s.cancel(); <-s.done }) }

func (s *admissionService) submit(ctx context.Context, command admissionCommand) (admissionRecord, bool, error) {
	if command.ChannelID == "" || !command.Owner.valid() || command.BuildRequest == nil {
		return admissionRecord{}, false, errors.New("admission: incomplete command")
	}
	digest, err := channel.Digest(command.Intent)
	if err != nil {
		return admissionRecord{}, false, err
	}
	tx, err := s.app.db.BeginTx(ctx, nil)
	if err != nil {
		return admissionRecord{}, false, err
	}
	defer tx.Rollback()
	if command.OperationID != "" {
		var existingDigest, existingChannel, existingOp, existingPrincipal, existingActor string
		err := tx.QueryRowContext(ctx, `SELECT request_digest,channel_id,op,COALESCE(requested_by_principal,''),COALESCE(requested_by_actor_id,'') FROM channel_admission_operations WHERE operation_id=?`, command.OperationID).
			Scan(&existingDigest, &existingChannel, &existingOp, &existingPrincipal, &existingActor)
		if err == nil {
			if existingDigest != digest || existingChannel != string(command.ChannelID) || existingOp != command.Op || !command.Owner.matches(existingPrincipal, existingActor) {
				return admissionRecord{}, true, errIdempotencyConflict
			}
			_ = tx.Rollback()
			return s.load(ctx, command.OperationID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return admissionRecord{}, false, err
		}
	}
	if command.IdempotencyKey != "" {
		var existing, existingDigest, existingChannel, existingOp string
		query, args := admissionIdempotencyLookup(command.Owner, command.ChannelID, command.IdempotencyKey)
		err := tx.QueryRowContext(ctx, query, args...).Scan(&existing, &existingDigest, &existingChannel, &existingOp)
		if err == nil {
			if existingDigest != digest || existingChannel != string(command.ChannelID) || existingOp != command.Op {
				return admissionRecord{}, true, errIdempotencyConflict
			}
			_ = tx.Rollback()
			return s.load(ctx, existing)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return admissionRecord{}, false, err
		}
	}
	operationID := command.OperationID
	if operationID == "" {
		operationID = "adm:v1:" + uuid.NewString()
	}
	request := command.BuildRequest(operationID)
	raw, err := json.Marshal(request)
	if err != nil {
		return admissionRecord{}, false, err
	}
	now := time.Now().UnixMilli()
	var idem any
	if command.IdempotencyKey != "" {
		idem = command.IdempotencyKey
	}
	principal, actorID := command.Owner.values()
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_admission_operations(operation_id,idempotency_key,channel_id,op,requested_by_principal,requested_by_actor_id,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?,?,?)`, operationID, idem, string(command.ChannelID), command.Op, principal, actorID, string(raw), digest, now); err != nil {
		if command.OperationID != "" {
			_ = tx.Rollback()
			record, found, loadErr := s.load(ctx, command.OperationID)
			if loadErr != nil || !found {
				return admissionRecord{}, false, errors.Join(err, loadErr)
			}
			if record.RequestDigest != digest || record.ChannelID != string(command.ChannelID) || record.Op != command.Op || !command.Owner.matches(record.RequestedByPrincipal, record.RequestedByActorID) {
				return admissionRecord{}, true, errIdempotencyConflict
			}
			return record, true, nil
		}
		if command.IdempotencyKey != "" {
			_ = tx.Rollback()
			return s.loadIdempotent(ctx, command.Owner, command.IdempotencyKey, digest, command.ChannelID, command.Op)
		}
		return admissionRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return admissionRecord{}, false, err
	}
	deliverCtx, cancel := context.WithTimeout(s.ctx, admissionSyncWindow)
	_ = s.runOperation(deliverCtx, operationID)
	cancel()
	s.notify()
	loadCtx := ctx
	if ctx.Err() != nil {
		loadCtx = context.Background()
	}
	record, _, err := s.load(loadCtx, operationID)
	return record, false, err
}

func admissionIdempotencyLookup(owner admissionOwner, channelID channel.ID, key string) (string, []any) {
	if owner.Principal != "" {
		return `SELECT operation_id,request_digest,channel_id,op FROM channel_admission_operations WHERE requested_by_principal=? AND idempotency_key=?`, []any{owner.Principal, key}
	}
	return `SELECT operation_id,request_digest,channel_id,op FROM channel_admission_operations WHERE channel_id=? AND requested_by_actor_id=? AND idempotency_key=?`, []any{string(channelID), string(owner.ActorID), key}
}

func (s *admissionService) loadIdempotent(ctx context.Context, owner admissionOwner, key, digest string, channelID channel.ID, op string) (admissionRecord, bool, error) {
	var operationID, existingDigest, existingChannel, existingOp string
	query, args := admissionIdempotencyLookup(owner, channelID, key)
	err := s.app.db.QueryRowContext(ctx, query, args...).Scan(&operationID, &existingDigest, &existingChannel, &existingOp)
	if err != nil {
		return admissionRecord{}, false, err
	}
	if existingDigest != digest || existingChannel != string(channelID) || existingOp != op {
		return admissionRecord{}, true, errIdempotencyConflict
	}
	return s.load(ctx, operationID)
}

func (s *admissionService) load(ctx context.Context, operationID string) (admissionRecord, bool, error) {
	var record admissionRecord
	err := s.app.db.QueryRowContext(ctx, `SELECT operation_id,channel_id,op,COALESCE(requested_by_principal,''),COALESCE(requested_by_actor_id,''),request_json,request_digest,status,result_json,error_code,created_at,done_at FROM channel_admission_operations WHERE operation_id=?`, operationID).
		Scan(&record.OperationID, &record.ChannelID, &record.Op, &record.RequestedByPrincipal, &record.RequestedByActorID, &record.RequestJSON, &record.RequestDigest, &record.Status, &record.ResultJSON, &record.ErrorCode, &record.CreatedAt, &record.DoneAt)
	if errors.Is(err, sql.ErrNoRows) {
		return admissionRecord{}, false, nil
	}
	return record, err == nil, err
}

func (s *admissionService) drain() {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	for range admissionDrain {
		id, ok := s.next()
		if !ok {
			return
		}
		_ = s.runOperation(s.ctx, id)
	}
}

func (s *admissionService) next() (string, bool) {
	now := time.Now().UnixMilli()
	query := `SELECT rowid,operation_id FROM channel_admission_operations WHERE status='pending' AND next_attempt_at<=? AND rowid>? ORDER BY rowid LIMIT 1`
	var rowID int64
	var id string
	err := s.app.db.QueryRowContext(s.ctx, query, now, s.cursor).Scan(&rowID, &id)
	if errors.Is(err, sql.ErrNoRows) && s.cursor != 0 {
		s.cursor = 0
		err = s.app.db.QueryRowContext(s.ctx, query, now, 0).Scan(&rowID, &id)
	}
	if err != nil {
		return "", false
	}
	s.cursor = rowID
	return id, true
}

func (s *admissionService) runOperation(ctx context.Context, operationID string) error {
	record, found, err := s.load(ctx, operationID)
	if err != nil || !found || record.Status != "pending" {
		return err
	}
	release := s.app.channelLocks.lock(record.ChannelID)
	defer release()
	return s.runOperationLocked(ctx, operationID)
}

// runOperationLocked is runOperation's body for a caller already inside the
// channel critical section.
func (s *admissionService) runOperationLocked(ctx context.Context, operationID string) error {
	record, found, err := s.load(ctx, operationID)
	if err != nil || !found || record.Status != "pending" {
		return err
	}
	var directoryPresent bool
	if err := s.app.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, record.ChannelID).Scan(&directoryPresent); err != nil {
		return s.retry(ctx, record, channel.ErrCodeInternal, err)
	}
	if !directoryPresent {
		return s.finish(ctx, record, "unresolved", nil, "channel_retired")
	}
	bundle, ok := s.app.host.Acquire(channel.ID(record.ChannelID))
	if !ok {
		return s.retry(ctx, record, channel.ErrCodeChannelUnavailable, errors.New("channel unavailable"))
	}
	result, callErr := s.deliver(ctx, bundle, record)
	if callErr != nil {
		var operationErr *channel.OperationError
		if errors.As(callErr, &operationErr) && !operationErr.Retryable {
			err := s.finish(ctx, record, "rejected", nil, string(operationErr.Code))
			if operationErr.Code == channel.ErrCodeRefConflict {
				s.app.logger.Error("admission.ref_conflict", "operation", operationID, "channel", record.ChannelID)
			}
			return errors.Join(callErr, err)
		}
		code := channel.ErrCodeInternal
		if errors.As(callErr, &operationErr) {
			code = operationErr.Code
		}
		return s.retry(ctx, record, code, callErr)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return s.retry(ctx, record, channel.ErrCodeInternal, err)
	}
	return s.finish(ctx, record, "done", raw, "")
}

func (s *admissionService) deliver(ctx context.Context, bundle channelhost.Bundle, record admissionRecord) (any, error) {
	switch record.Op {
	case "join":
		var request channel.AdmitRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().Admit(ctx, request)
	case "introduce":
		var request channel.IntroduceRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().Introduce(ctx, request)
	case "remove":
		var request channel.RemoveRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().Remove(ctx, request)
	case "attach":
		var request channel.DaemonRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		// Delivery acts on present realm state, never on the state remembered
		// at submission: attach establishes a reference, so its referent must
		// be currently registered — a pending attach must not revive a daemon
		// tombstoned while it waited. Detach stays unchecked (removing a
		// reference to a dead referent is always legal). The channel lock held
		// A concurrent tombstone after this check is harmless: the Home pull
		// arm observes it and removes any binding that slipped through.
		var present bool
		if err := s.app.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM daemons WHERE id=? AND deleted_at IS NULL)`, request.DaemonID).Scan(&present); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeInternal, Detail: err.Error(), Retryable: true}
		}
		if !present {
			return nil, &channel.OperationError{Code: admissionCodeDaemonNotFound, Detail: request.DaemonID}
		}
		return bundle.SysOp().AttachDaemon(ctx, request)
	case "detach":
		var request channel.DaemonRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().DetachDaemon(ctx, request)
	default:
		return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: fmt.Sprintf("unknown op %q", record.Op)}
	}
}

func (s *admissionService) retry(ctx context.Context, record admissionRecord, code channel.OperationErrorCode, cause error) error {
	var attempt int
	_ = s.app.db.QueryRowContext(ctx, `SELECT attempt FROM channel_admission_operations WHERE operation_id=?`, record.OperationID).Scan(&attempt)
	next := time.Now().Add(backoff(attempt + 1)).UnixMilli()
	_, err := s.app.db.ExecContext(ctx, `UPDATE channel_admission_operations SET attempt=attempt+1,error_code=?,next_attempt_at=? WHERE operation_id=? AND status='pending'`, string(code), next, record.OperationID)
	return errors.Join(cause, err)
}

func (s *admissionService) finish(ctx context.Context, record admissionRecord, status string, result json.RawMessage, code string) error {
	tx, err := s.app.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	var raw any
	if result != nil {
		raw = string(result)
	}
	var errorCode any
	if code != "" {
		errorCode = code
	}
	res, err := tx.ExecContext(ctx, `UPDATE channel_admission_operations SET status=?,attempt=attempt+1,result_json=?,error_code=?,done_at=? WHERE operation_id=? AND status='pending'`, status, raw, errorCode, now, record.OperationID)
	if err != nil {
		return err
	}
	changed, err := res.RowsAffected()
	if err != nil || changed == 0 {
		return err
	}
	projectionPrincipal := ""
	if status == "done" && record.Op == "join" && result != nil {
		var request channel.AdmitRequest
		var admitted channel.AdmitResult
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return err
		}
		if err := json.Unmarshal(result, &admitted); err != nil {
			return err
		}
		projectionPrincipal = request.Principal
		if _, err := tx.ExecContext(ctx, `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at) VALUES (?,?,?,?) ON CONFLICT(principal,channel_id) DO UPDATE SET actor_id=excluded.actor_id,updated_at=excluded.updated_at`, request.Principal, record.ChannelID, string(admitted.ActorID), now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if projectionPrincipal != "" && s.app.membershipPoke != nil {
		s.app.membershipPoke(projectionPrincipal)
	}
	return nil
}

// admissionCodeDaemonNotFound is realm-side admission vocabulary (the account's
// error_code column), not a membrane operate-frame code — the frame closed set
// does not grow for a rejection decided before any frame is minted.
const admissionCodeDaemonNotFound channel.OperationErrorCode = "daemon_not_found"

func admissionErrorHTTP(code string) int {
	switch channel.OperationErrorCode(code) {
	case admissionCodeDaemonNotFound:
		return 404
	case channel.ErrCodeBadPayload, channel.ErrCodeUnknownClass, channel.ErrCodeInvalidDesiredHost:
		return 400
	case channel.ErrCodeForbidden, channel.ErrCodeNotAcceptedSource:
		return 403
	case channel.ErrCodeDeclNotFound:
		return 404
	case channel.ErrCodeMemberInactive, channel.ErrCodeNotInComposition, channel.ErrCodeProtectedActor, channel.ErrCodeRefConflict:
		return 409
	default:
		return 503
	}
}

func respondAdmissionRecord(c *gin.Context, record admissionRecord, err error, successStatus int) {
	if errors.Is(err, errIdempotencyConflict) {
		c.JSON(409, gin.H{"error": "idempotency conflict"})
		return
	}
	if err != nil {
		var operationErr *channel.OperationError
		if errors.As(err, &operationErr) {
			c.JSON(admissionErrorHTTP(string(operationErr.Code)), gin.H{
				"error":      operationErr.Detail,
				"error_code": operationErr.Code,
			})
			return
		}
		c.JSON(500, gin.H{"error": "admission failed"})
		return
	}
	view := gin.H{"operation_id": record.OperationID, "status": record.Status}
	if record.ResultJSON.Valid {
		view["result_json"] = json.RawMessage(record.ResultJSON.String)
	}
	if record.ErrorCode.Valid {
		view["error_code"] = record.ErrorCode.String
	}
	switch record.Status {
	case "pending":
		c.JSON(202, view)
	case "rejected":
		c.JSON(admissionErrorHTTP(record.ErrorCode.String), view)
	case "unresolved":
		c.JSON(409, view)
	default:
		c.JSON(successStatus, view)
	}
}
