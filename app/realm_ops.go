package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type realmOps struct{ app *App }

// realmResourceCopyLimitBytes is reference-realm policy. The membrane and its
// realm-tool cell do not invent transport limits; they consume the stream and
// propagate this realm-owned rejection unchanged.
const realmResourceCopyLimitBytes int64 = 32 << 20

func (o realmOps) requesterFacts(ctx context.Context, req channel.Requester) (channel.ActorFacts, error) {
	if req.ActorID == "" || req.ChannelID == "" || req.RequestID == "" {
		return channel.ActorFacts{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "incomplete requester"}
	}
	bundle, ok := o.app.host.Acquire(req.ChannelID)
	if !ok {
		return channel.ActorFacts{}, &channel.RealmError{Code: channel.RealmChannelUnavailable}
	}
	facts, found, err := bundle.View().ActorFacts(ctx, req.ActorID)
	if err != nil {
		return channel.ActorFacts{}, err
	}
	if !found || !facts.Active {
		return channel.ActorFacts{}, &channel.RealmError{Code: channel.RealmForbidden, Detail: "requester is not active"}
	}
	return facts, nil
}

func scanDecl(row interface{ Scan(...any) error }) (channel.DeclDetail, error) {
	var d channel.DeclDetail
	var config sql.NullString
	err := row.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &config, &d.Visibility)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if config.Valid && config.String != "" {
		d.Config = json.RawMessage(config.String)
	}
	return d, nil
}

func (o realmOps) ListDeclarations(ctx context.Context, req channel.Requester) ([]channel.DeclSummary, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return nil, err
	}
	rows, err := o.app.db.QueryContext(ctx, `SELECT id,name,owner,default_class,visibility FROM actor_decls WHERE deleted_at IS NULL AND (visibility='public' OR owner=?) ORDER BY created_at,id`, facts.Principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []channel.DeclSummary{}
	for rows.Next() {
		var d channel.DeclSummary
		if err := rows.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &d.Visibility); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (o realmOps) InspectDeclaration(ctx context.Context, req channel.Requester, declID string) (channel.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	d, err := scanDecl(o.app.db.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,visibility FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID))
	if errors.Is(err, sql.ErrNoRows) {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmDeclNotFound}
	}
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if d.Visibility != "public" && d.Owner != facts.Principal {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmForbidden}
	}
	return d, nil
}

func normalizeDeclSpec(spec channel.DeclSpec) (channel.DeclSpec, error) {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.Class = strings.TrimSpace(spec.Class)
	if spec.Name == "" || spec.Class == "" {
		return spec, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "name and class required"}
	}
	if spec.Visibility == "" {
		spec.Visibility = "private"
	}
	if spec.Visibility != "public" && spec.Visibility != "private" {
		return spec, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "invalid visibility"}
	}
	if len(spec.Config) > 0 && !isJSONObject(spec.Config) {
		return spec, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "config must be an object"}
	}
	return spec, nil
}

func (o realmOps) CreateDeclaration(ctx context.Context, req channel.Requester, spec channel.DeclSpec) (channel.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmForbidden, Detail: "only humans may write the realm declaration registry"}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if _, ok, err := (compositionResolver{app: o.app}).ClassKind(ctx, spec.Class); err != nil || !ok || spec.Class == realmToolClass {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "unknown or reserved class"}
	}
	id := uuid.NewString()
	now := time.Now().UnixMilli()
	var config any
	if len(spec.Config) > 0 {
		config = string(spec.Config)
	}
	_, err = o.app.db.ExecContext(ctx, `INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`, id, spec.Name, facts.Principal, spec.Class, config, now, now, spec.Visibility)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	return channel.DeclDetail{DeclSummary: channel.DeclSummary{ID: id, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o realmOps) EditDeclaration(ctx context.Context, req channel.Requester, declID string, spec channel.DeclSpec) (channel.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmForbidden}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if _, ok, err := (compositionResolver{app: o.app}).ClassKind(ctx, spec.Class); err != nil || !ok || spec.Class == realmToolClass {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "unknown or reserved class"}
	}
	tx, err := o.app.db.BeginTx(ctx, nil)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	defer tx.Rollback()
	var currentClass string
	if err := tx.QueryRowContext(ctx, `SELECT default_class FROM actor_decls WHERE id=? AND owner=? AND deleted_at IS NULL`, declID, facts.Principal).Scan(&currentClass); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmDeclNotFound}
		}
		return channel.DeclDetail{}, err
	}
	oldKind, oldFound, err := (compositionResolver{app: o.app}).ClassKind(ctx, currentClass)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	newKind, newFound, err := (compositionResolver{app: o.app}).ClassKind(ctx, spec.Class)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if !oldFound || !newFound || oldKind != newKind {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "class must remain within the declaration kind"}
	}
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `UPDATE actor_decls SET name=?,default_class=?,config_json=?,visibility=?,updated_at=? WHERE id=? AND owner=? AND deleted_at IS NULL`, spec.Name, spec.Class, string(spec.Config), spec.Visibility, now, declID, facts.Principal)
	if err != nil {
		return channel.DeclDetail{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return channel.DeclDetail{}, &channel.RealmError{Code: channel.RealmDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return channel.DeclDetail{}, err
	}
	o.app.pokeAllChannels(ctx)
	return channel.DeclDetail{DeclSummary: channel.DeclSummary{ID: declID, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o realmOps) RevokeDeclaration(ctx context.Context, req channel.Requester, declID string) error {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return err
	}
	if facts.Kind != actor.KindHuman {
		return &channel.RealmError{Code: channel.RealmForbidden}
	}
	tx, err := o.app.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `UPDATE actor_decls SET deleted_at=?,updated_at=? WHERE id=? AND owner=? AND deleted_at IS NULL`, now, now, declID, facts.Principal)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &channel.RealmError{Code: channel.RealmDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (o realmOps) Introduce(ctx context.Context, req channel.Requester, declID string, _ channel.IntroduceOpts) (channel.IntroduceResult, error) {
	_, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	var visibility string
	if err := o.app.db.QueryRowContext(ctx, `SELECT visibility FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID).Scan(&visibility); errors.Is(err, sql.ErrNoRows) {
		return channel.IntroduceResult{}, &channel.RealmError{Code: channel.RealmDeclNotFound}
	} else if err != nil {
		return channel.IntroduceResult{}, err
	}
	if visibility != "public" {
		return channel.IntroduceResult{}, &channel.RealmError{Code: channel.RealmForbidden}
	}
	ref := channel.DerivedRealmToolRef(req.ChannelID, req.RequestID)
	record, _, err := o.app.admission.submit(ctx, admissionCommand{
		ChannelID: req.ChannelID, Op: "introduce", Owner: actorAdmissionOwner(req.ActorID),
		OperationID: ref, Intent: struct {
			DeclID string `json:"decl_id"`
		}{declID},
		BuildRequest: func(string) any {
			return channel.IntroduceRequest{Ref: ref, DeclID: declID, InitiatorActorID: req.ActorID}
		},
	})
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	switch record.Status {
	case "done":
		var result channel.IntroduceResult
		if err := json.Unmarshal([]byte(record.ResultJSON.String), &result); err != nil {
			return result, err
		}
		return result, nil
	case "rejected":
		return channel.IntroduceResult{}, &channel.RealmError{Code: admissionRealmErrorCode(record.ErrorCode.String), Detail: record.ErrorCode.String}
	default:
		return channel.IntroduceResult{}, &channel.ErrResultUnknown{Ref: ref}
	}
}

// introduceRealmErrorCode maps a rejected admission operation's operate error
// code onto the frozen RealmOps error closed set (spec S4-9:
// forbidden / decl_not_found / resource_not_found / capability_unavailable /
// channel_unavailable / realm_unavailable / invalid_request / conflict). The
// grouping follows the same semantic classes admissionErrorHTTP uses. Codes with
// no closed-set counterpart keep RealmForbidden; the original operate code always
// rides along in Detail (both here and at the callsite), so nothing is lost.
func admissionRealmErrorCode(code string) channel.RealmErrorCode {
	switch channel.OperationErrorCode(code) {
	case channel.ErrCodeDeclNotFound:
		return channel.RealmDeclNotFound
	case channel.ErrCodeBadPayload, channel.ErrCodeUnknownClass, channel.ErrCodeInvalidDesiredHost:
		return channel.RealmInvalidRequest
	case channel.ErrCodeChannelUnavailable:
		return channel.RealmChannelUnavailable
	case channel.ErrCodeProtectedActor, channel.ErrCodeNotInComposition, channel.ErrCodeMemberInactive, channel.ErrCodeRefConflict:
		return channel.RealmConflict
	default:
		// forbidden / not_accepted_source and any transient
		// code that has no RealmOps closed-set counterpart stay RealmForbidden.
		return channel.RealmForbidden
	}
}

func (o realmOps) Remove(ctx context.Context, req channel.Requester, target actor.ActorID) (channel.RemoveResult, error) {
	if _, err := o.requesterFacts(ctx, req); err != nil {
		return channel.RemoveResult{}, err
	}
	if target == "" {
		return channel.RemoveResult{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "target required"}
	}
	ref := channel.DerivedRealmToolRef(req.ChannelID, req.RequestID)
	record, _, err := o.app.admission.submit(ctx, admissionCommand{
		ChannelID: req.ChannelID, Op: "remove", Owner: actorAdmissionOwner(req.ActorID), OperationID: ref,
		Intent: struct {
			Target actor.ActorID `json:"target"`
		}{target},
		BuildRequest: func(string) any {
			return channel.RemoveRequest{Ref: ref, Target: target, InitiatorActorID: req.ActorID}
		},
	})
	if err != nil {
		return channel.RemoveResult{}, err
	}
	switch record.Status {
	case "done":
		var result channel.RemoveResult
		if err := json.Unmarshal([]byte(record.ResultJSON.String), &result); err != nil {
			return result, err
		}
		return result, nil
	case "rejected":
		return channel.RemoveResult{}, &channel.RealmError{Code: admissionRealmErrorCode(record.ErrorCode.String), Detail: record.ErrorCode.String}
	default:
		return channel.RemoveResult{}, &channel.ErrResultUnknown{Ref: ref}
	}
}

func (o realmOps) OperationStatus(ctx context.Context, req channel.Requester, ref string) (channel.OperationView, error) {
	_, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.OperationView{}, err
	}
	record, found, err := o.app.admission.load(ctx, ref)
	if err != nil {
		return channel.OperationView{}, err
	}
	if !found {
		return channel.OperationView{}, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "operation not found"}
	}
	if record.ChannelID != string(req.ChannelID) || record.RequestedByActorID != string(req.ActorID) {
		return channel.OperationView{}, &channel.RealmError{Code: channel.RealmForbidden}
	}
	view := channel.OperationView{Ref: ref, Family: "admission", Status: record.Status, Op: record.Op, CreatedAt: record.CreatedAt}
	if record.ResultJSON.Valid {
		view.ResultJSON = json.RawMessage(record.ResultJSON.String)
	}
	if record.ErrorCode.Valid {
		view.ErrorCode = record.ErrorCode.String
	}
	if record.DoneAt.Valid {
		done := record.DoneAt.Int64
		view.DoneAt = &done
	}
	return view, nil
}

func (o realmOps) crossReader(ctx context.Context, req channel.Requester, source channel.ID) (channel.Reader, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return channel.Reader{}, err
	}
	bundle, ok := o.app.host.Acquire(source)
	if !ok {
		return channel.Reader{}, &channel.RealmError{Code: channel.RealmChannelUnavailable}
	}
	toolRows, err := bundle.View().DeclaredBySource(ctx, realmToolDeclID)
	if err != nil {
		return channel.Reader{}, err
	}
	if len(toolRows) == 0 {
		return channel.Reader{}, &channel.RealmError{Code: channel.RealmCapabilityUnavailable, Detail: "source realm tool absent"}
	}
	if source == req.ChannelID {
		return channel.Reader{ActorID: req.ActorID, Mode: channel.ReaderMember}, nil
	}
	if id, found, err := bundle.View().ResolvePrincipal(ctx, actor.KindHuman, facts.Principal); err != nil {
		return channel.Reader{}, err
	} else if found {
		return channel.Reader{ActorID: id, Mode: channel.ReaderMember}, nil
	}
	_, reader, reason, err := o.app.canObserve(ctx, source, facts.Principal)
	if err != nil {
		return channel.Reader{}, err
	}
	if reason != observeAllowed {
		code := channel.RealmCapabilityUnavailable
		if reason == observeChannelAbsent {
			code = channel.RealmChannelUnavailable
		}
		return channel.Reader{}, &channel.RealmError{Code: code, Detail: string(reason)}
	}
	return reader, nil
}

func (o realmOps) ListResources(ctx context.Context, req channel.Requester, source channel.ID, q channel.ResourceListQuery) (channel.ResourcePage, error) {
	reader, err := o.crossReader(ctx, req, source)
	if err != nil {
		return channel.ResourcePage{}, err
	}
	bundle, ok := o.app.host.Acquire(source)
	if !ok {
		return channel.ResourcePage{}, &channel.RealmError{Code: channel.RealmChannelUnavailable}
	}
	return bundle.View().Resources().List(ctx, reader, q)
}

func (o realmOps) FetchResource(ctx context.Context, req channel.Requester, source channel.ID, id resource.ResourceID) (channel.ResourceFetch, error) {
	reader, err := o.crossReader(ctx, req, source)
	if err != nil {
		return channel.ResourceFetch{}, err
	}
	bundle, ok := o.app.host.Acquire(source)
	if !ok {
		return channel.ResourceFetch{}, &channel.RealmError{Code: channel.RealmChannelUnavailable}
	}
	fetched, err := bundle.View().Resources().Fetch(ctx, reader, id)
	if err != nil {
		return channel.ResourceFetch{}, err
	}
	if reader.Mode == channel.ReaderObserver {
		fetched.Body = &observerResourceBody{
			ctx: ctx, app: o.app, source: source, principal: reader.Principal, body: fetched.Body,
		}
	}
	fetched.Body = newRealmCopyPolicyBody(fetched.Body, realmResourceCopyLimitBytes)
	return fetched, nil
}

type realmCopyPolicyBody struct {
	body      io.ReadCloser
	remaining int64
}

func newRealmCopyPolicyBody(body io.ReadCloser, limit int64) io.ReadCloser {
	return &realmCopyPolicyBody{body: body, remaining: limit}
}

func (r *realmCopyPolicyBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining < 0 {
		return 0, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.body.Read(probe[:])
		if n > 0 {
			return 0, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		return n, &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
	}
	return n, err
}

func (r *realmCopyPolicyBody) Close() error { return r.body.Close() }

// observerResourceBody binds a cross-membrane byte stream to its realm gate.
// Each bounded chunk rechecks the source sovereignty switch; revocation may let
// the in-flight chunk finish, but no subsequent chunk crosses the boundary.
type observerResourceBody struct {
	ctx       context.Context
	app       *App
	source    channel.ID
	principal string
	body      io.ReadCloser
}

func (r *observerResourceBody) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	_, _, reason, err := r.app.canObserve(r.ctx, r.source, r.principal)
	if err != nil {
		return 0, err
	}
	if reason != observeAllowed {
		code := channel.RealmCapabilityUnavailable
		if reason == observeChannelAbsent {
			code = channel.RealmChannelUnavailable
		}
		return 0, &channel.RealmError{Code: code, Detail: string(reason)}
	}
	const chunk = 32 * 1024
	if len(p) > chunk {
		p = p[:chunk]
	}
	return r.body.Read(p)
}

func (r *observerResourceBody) Close() error { return r.body.Close() }
