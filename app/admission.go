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
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	admissionDrain      = 16
	admissionSyncWindow = 2 * time.Second
)

var errIdempotencyConflict = errors.New("admission: idempotency key reused with different request")

type admissionCommand struct {
	ChannelID      channel.ID
	Op             string
	RequestedBy    string
	OperationID    string
	IdempotencyKey string
	Intent         any
	BuildRequest   func(string) any
}

type admissionRecord struct {
	OperationID, ChannelID, Op, RequestedBy, RequestJSON, RequestDigest, Status string
	ResultJSON, ErrorCode                                                       sql.NullString
	CreatedAt                                                                   int64
	DoneAt                                                                      sql.NullInt64
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
	if command.ChannelID == "" || command.RequestedBy == "" || command.BuildRequest == nil {
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
		var existingDigest, existingChannel, existingOp, existingRequester string
		err := tx.QueryRowContext(ctx, `SELECT request_digest,channel_id,op,requested_by FROM channel_admission_operations WHERE operation_id=?`, command.OperationID).
			Scan(&existingDigest, &existingChannel, &existingOp, &existingRequester)
		if err == nil {
			if existingDigest != digest || existingChannel != string(command.ChannelID) || existingOp != command.Op || existingRequester != command.RequestedBy {
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
		err := tx.QueryRowContext(ctx, `SELECT operation_id,request_digest,channel_id,op FROM channel_admission_operations WHERE requested_by=? AND idempotency_key=?`, command.RequestedBy, command.IdempotencyKey).Scan(&existing, &existingDigest, &existingChannel, &existingOp)
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_admission_operations(operation_id,idempotency_key,channel_id,op,requested_by,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?,?)`, operationID, idem, string(command.ChannelID), command.Op, command.RequestedBy, string(raw), digest, now); err != nil {
		if command.OperationID != "" {
			_ = tx.Rollback()
			record, found, loadErr := s.load(ctx, command.OperationID)
			if loadErr != nil || !found {
				return admissionRecord{}, false, errors.Join(err, loadErr)
			}
			if record.RequestDigest != digest || record.ChannelID != string(command.ChannelID) || record.Op != command.Op || record.RequestedBy != command.RequestedBy {
				return admissionRecord{}, true, errIdempotencyConflict
			}
			return record, true, nil
		}
		if command.IdempotencyKey != "" {
			_ = tx.Rollback()
			return s.loadIdempotent(ctx, command.RequestedBy, command.IdempotencyKey, digest, string(command.ChannelID), command.Op)
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

func (s *admissionService) loadIdempotent(ctx context.Context, requestedBy, key, digest, channelID, op string) (admissionRecord, bool, error) {
	var operationID, existingDigest, existingChannel, existingOp string
	err := s.app.db.QueryRowContext(ctx, `SELECT operation_id,request_digest,channel_id,op FROM channel_admission_operations WHERE requested_by=? AND idempotency_key=?`, requestedBy, key).Scan(&operationID, &existingDigest, &existingChannel, &existingOp)
	if err != nil {
		return admissionRecord{}, false, err
	}
	if existingDigest != digest || existingChannel != channelID || existingOp != op {
		return admissionRecord{}, true, errIdempotencyConflict
	}
	return s.load(ctx, operationID)
}

func (s *admissionService) submitEdit(ctx context.Context, chID channel.ID, rawActorID, caller string, config json.RawMessage, idemKey string) (admissionRecord, error) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	bundle, ok := s.app.host.Acquire(chID)
	if !ok {
		return admissionRecord{}, errors.New("admission: channel unavailable")
	}
	actors, err := bundle.View().ActiveActors(ctx)
	if err != nil {
		return admissionRecord{}, err
	}
	var target storespec.ActorControlRow
	for _, row := range actors {
		if string(row.ID) == rawActorID {
			target = row
			break
		}
	}
	if target.ID == "" || target.SourceDeclID == "" {
		return admissionRecord{}, &channel.OperationError{Code: channel.ErrCodeNotInComposition, Detail: rawActorID}
	}
	facts, err := (compositionResolver{app: s.app}).ResolveDeclaration(ctx, chID, target.SourceDeclID)
	if err != nil {
		return admissionRecord{}, err
	}
	if facts.Visibility != "public" && facts.OwnerPrincipal != caller {
		return admissionRecord{}, &channel.OperationError{Code: channel.ErrCodeForbidden, Detail: "declaration is private"}
	}
	intent := struct {
		ActorID string          `json:"actor_id"`
		Config  json.RawMessage `json:"config"`
	}{rawActorID, config}
	digest, err := channel.Digest(intent)
	if err != nil {
		return admissionRecord{}, err
	}
	tx, err := s.app.db.BeginTx(ctx, nil)
	if err != nil {
		return admissionRecord{}, err
	}
	defer tx.Rollback()
	if idemKey != "" {
		var existing, existingDigest, existingChannel, existingOp string
		err := tx.QueryRowContext(ctx, `SELECT operation_id,request_digest,channel_id,op FROM channel_admission_operations WHERE requested_by=? AND idempotency_key=?`, caller, idemKey).Scan(&existing, &existingDigest, &existingChannel, &existingOp)
		if err == nil {
			if existingDigest != digest || existingChannel != string(chID) || existingOp != "edit" {
				return admissionRecord{}, errIdempotencyConflict
			}
			_ = tx.Rollback()
			record, _, err := s.load(ctx, existing)
			return record, err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return admissionRecord{}, err
		}
	}
	operationID := "adm:v1:" + uuid.NewString()
	if _, err := tx.ExecContext(ctx, `INSERT INTO decl_render_state(channel_id,decl_id,render_seq) VALUES (?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET render_seq=MAX(render_seq,excluded.render_seq)`, string(chID), target.SourceDeclID, target.RenderSeq); err != nil {
		return admissionRecord{}, err
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `UPDATE decl_render_state SET render_seq=render_seq+1 WHERE channel_id=? AND decl_id=? RETURNING render_seq`, string(chID), target.SourceDeclID).Scan(&seq); err != nil {
		return admissionRecord{}, err
	}
	now := time.Now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_decl_overlays(channel_id,decl_id,pending_config_json,pending_ref,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET pending_config_json=excluded.pending_config_json,pending_ref=excluded.pending_ref,updated_at=excluded.updated_at`, string(chID), target.SourceDeclID, string(config), operationID, now); err != nil {
		return admissionRecord{}, err
	}
	snapshot, err := (channel.RenderedSnapshot{
		Class: target.Class, Config: append(json.RawMessage(nil), config...),
		Placement: channel.Placement{Kind: channel.PlacementKind(target.Placement.Kind), DesiredHost: target.Placement.Host},
		TIdleMS:   target.TIdle.Milliseconds(), RenderSeq: seq,
	}).Seal()
	if err != nil {
		return admissionRecord{}, err
	}
	request := channel.ApplyDeclVersionRequest{
		Ref: operationID, DeclID: target.SourceDeclID, Rendered: snapshot,
		Authority: channel.AuthorityDelegate, InitiatorPrincipal: caller,
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return admissionRecord{}, err
	}
	var idem any
	if idemKey != "" {
		idem = idemKey
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_admission_operations(operation_id,idempotency_key,channel_id,op,requested_by,request_json,request_digest,created_at) VALUES (?,?,?,?,?,?,?,?)`, operationID, idem, string(chID), "edit", caller, string(raw), digest, now); err != nil {
		return admissionRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return admissionRecord{}, err
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
	return record, err
}

func (s *admissionService) load(ctx context.Context, operationID string) (admissionRecord, bool, error) {
	var record admissionRecord
	err := s.app.db.QueryRowContext(ctx, `SELECT operation_id,channel_id,op,requested_by,request_json,request_digest,status,result_json,error_code,created_at,done_at FROM channel_admission_operations WHERE operation_id=?`, operationID).
		Scan(&record.OperationID, &record.ChannelID, &record.Op, &record.RequestedBy, &record.RequestJSON, &record.RequestDigest, &record.Status, &record.ResultJSON, &record.ErrorCode, &record.CreatedAt, &record.DoneAt)
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
	record, found, err = s.load(ctx, operationID)
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
	case "attach":
		var request channel.DaemonRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().AttachDaemon(ctx, request)
	case "detach":
		var request channel.DaemonRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().DetachDaemon(ctx, request)
	case "edit":
		var request channel.ApplyDeclVersionRequest
		if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
			return nil, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
		}
		return bundle.SysOp().ApplyDeclVersion(ctx, request)
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
	if err := s.finishOverlayTx(ctx, tx, record, status == "done"); err != nil {
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

func (s *admissionService) finishOverlayTx(ctx context.Context, tx *sql.Tx, record admissionRecord, success bool) error {
	if record.Op != "edit" {
		return nil
	}
	if success {
		_, err := tx.ExecContext(ctx, `UPDATE channel_decl_overlays SET config_json=pending_config_json,pending_config_json=NULL,pending_ref=NULL WHERE channel_id=? AND pending_ref=?`, record.ChannelID, record.OperationID)
		return err
	} else {
		_, err := tx.ExecContext(ctx, `UPDATE channel_decl_overlays SET pending_config_json=NULL,pending_ref=NULL WHERE channel_id=? AND pending_ref=?`, record.ChannelID, record.OperationID)
		return err
	}
}

func (s *admissionService) deliverFinalize(ctx context.Context, bundle channelhost.Bundle, action string, payload json.RawMessage) error {
	switch action {
	case "apply":
		var request channel.ApplyDeclVersionRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			return err
		}
		_, err := bundle.SysOp().ApplyDeclVersion(ctx, request)
		return err
	case "revoke":
		var request channel.RevokeDeclRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			return err
		}
		_, err := bundle.SysOp().RevokeDeclTargets(ctx, request)
		return err
	default:
		return fmt.Errorf("unknown finalize action %q", action)
	}
}

func admissionErrorHTTP(code string) int {
	switch channel.OperationErrorCode(code) {
	case channel.ErrCodeBadPayload, channel.ErrCodeInvalidPlacement, channel.ErrCodeUnknownClass, channel.ErrCodeInvalidDesiredHost:
		return 400
	case channel.ErrCodeForbidden, channel.ErrCodeUnauthorizedSender, channel.ErrCodeNotAcceptedSource:
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
