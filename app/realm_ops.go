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

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/realmtool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

type realmOps struct{ app *App }

// realmResourceCopyLimitBytes is reference-realm policy. The membrane and its
// realm-tool cell do not invent transport limits; they consume the stream and
// propagate this realm-owned rejection unchanged.
const realmResourceCopyLimitBytes int64 = 32 << 20

func (o realmOps) requesterFacts(ctx context.Context, req realmtool.Requester) (channelspec.ActorFacts, error) {
	if req.ActorID == "" || req.ChannelID == "" || req.RequestID == "" {
		return channelspec.ActorFacts{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "incomplete requester"}
	}
	bundle, ok := o.app.host.Acquire(req.ChannelID)
	if !ok {
		return channelspec.ActorFacts{}, &channelspec.RealmError{Code: channelspec.RealmChannelUnavailable}
	}
	facts, found, err := bundle.View().ActorFacts(ctx, req.ActorID)
	if err != nil {
		return channelspec.ActorFacts{}, err
	}
	if !found || !facts.Active {
		return channelspec.ActorFacts{}, &channelspec.RealmError{Code: channelspec.RealmForbidden, Detail: "requester is not active"}
	}
	return facts, nil
}

func scanDecl(row interface{ Scan(...any) error }) (realmtool.DeclDetail, error) {
	var d realmtool.DeclDetail
	var config sql.NullString
	err := row.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &config, &d.Visibility)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if config.Valid && config.String != "" {
		d.Config = json.RawMessage(config.String)
	}
	return d, nil
}

func (o realmOps) ListDeclarations(ctx context.Context, req realmtool.Requester) ([]realmtool.DeclSummary, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return nil, err
	}
	rows, err := o.app.db.QueryContext(ctx, `SELECT id,name,owner,default_class,visibility FROM actor_decls WHERE deleted_at IS NULL AND (visibility='public' OR owner=?) ORDER BY created_at,id`, facts.Principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []realmtool.DeclSummary{}
	for rows.Next() {
		var d realmtool.DeclSummary
		if err := rows.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &d.Visibility); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (o realmOps) InspectDeclaration(ctx context.Context, req realmtool.Requester, declID string) (realmtool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	d, err := scanDecl(o.app.db.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,visibility FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID))
	if errors.Is(err, sql.ErrNoRows) {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmDeclNotFound}
	}
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if !declarationVisibleTo(d.Visibility, d.Owner, facts.Principal) {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmForbidden}
	}
	return d, nil
}

func normalizeDeclSpec(spec realmtool.DeclSpec) (realmtool.DeclSpec, error) {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.Class = strings.TrimSpace(spec.Class)
	if spec.Name == "" || spec.Class == "" {
		return spec, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "name and class required"}
	}
	if spec.Visibility == "" {
		spec.Visibility = "private"
	}
	if spec.Visibility != "public" && spec.Visibility != "private" {
		return spec, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "invalid visibility"}
	}
	if len(spec.Config) > 0 && !isJSONObject(spec.Config) {
		return spec, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "config must be an object"}
	}
	return spec, nil
}

func (o realmOps) CreateDeclaration(ctx context.Context, req realmtool.Requester, spec realmtool.DeclSpec) (realmtool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmForbidden, Detail: "only humans may write the realm declaration registry"}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if _, ok, err := o.app.declarationClassKind(ctx, spec.Class); err != nil || !ok {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "unknown or reserved class"}
	}
	if err := registry.ValidateConfig(spec.Class, spec.Config); err != nil {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "config_invalid"}
	}
	id := uuid.NewString()
	now := time.Now().UnixMilli()
	var config any
	if len(spec.Config) > 0 {
		config = string(spec.Config)
	}
	_, err = o.app.db.ExecContext(ctx, `INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`, id, spec.Name, facts.Principal, spec.Class, config, now, now, spec.Visibility)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	return realmtool.DeclDetail{DeclSummary: realmtool.DeclSummary{ID: id, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o realmOps) EditDeclaration(ctx context.Context, req realmtool.Requester, declID string, spec realmtool.DeclSpec) (realmtool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmForbidden}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if _, ok, err := o.app.declarationClassKind(ctx, spec.Class); err != nil || !ok {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "unknown or reserved class"}
	}
	if err := registry.ValidateConfig(spec.Class, spec.Config); err != nil {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "config_invalid"}
	}
	tx, err := o.app.db.BeginTx(ctx, nil)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	defer tx.Rollback()
	var currentClass string
	if err := tx.QueryRowContext(ctx, `SELECT default_class FROM actor_decls WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, declID, facts.Principal).Scan(&currentClass); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmDeclNotFound}
		}
		return realmtool.DeclDetail{}, err
	}
	sameKind, err := o.app.declarationClassTransition(ctx, currentClass, spec.Class)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if !sameKind {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "class must remain within the declaration kind"}
	}
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `UPDATE actor_decls SET name=?,default_class=?,config_json=?,visibility=?,updated_at=? WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, spec.Name, spec.Class, string(spec.Config), spec.Visibility, now, declID, facts.Principal)
	if err != nil {
		return realmtool.DeclDetail{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return realmtool.DeclDetail{}, &channelspec.RealmError{Code: channelspec.RealmDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return realmtool.DeclDetail{}, err
	}
	o.app.pokeAllChannels(ctx)
	return realmtool.DeclDetail{DeclSummary: realmtool.DeclSummary{ID: declID, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o realmOps) RevokeDeclaration(ctx context.Context, req realmtool.Requester, declID string) error {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return err
	}
	if facts.Kind != actor.KindHuman {
		return &channelspec.RealmError{Code: channelspec.RealmForbidden}
	}
	tx, err := o.app.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `UPDATE actor_decls SET deleted_at=?,updated_at=? WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, now, now, declID, facts.Principal)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &channelspec.RealmError{Code: channelspec.RealmDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (o realmOps) Introduce(ctx context.Context, req realmtool.Requester, declID string, _ realmtool.IntroduceOpts) (channel.IntroduceResult, error) {
	ref := realmtool.DerivedRealmToolRef(req.ChannelID, req.RequestID)
	outcome, err := forwardSysop(ctx, o.app, req.ChannelID, sysopForward[channel.IntroduceResult]{
		Predicate: func(bundle channelhost.Bundle) (channel.IntroduceResult, bool, error) {
			instances, err := bundle.View().DeclaredInstances(ctx, declID)
			if len(instances) != 0 {
				return channel.IntroduceResult{ActorID: instances[0], Created: false}, true, err
			}
			return channel.IntroduceResult{}, false, err
		},
		Qualify: func(bundle channelhost.Bundle) error {
			facts, found, err := bundle.View().ActorFacts(ctx, req.ActorID)
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if !found || !facts.Active {
				return &sysopGateError{Status: 403, Code: "forbidden"}
			}
			var visibility string
			err = o.app.db.QueryRowContext(ctx,
				`SELECT visibility FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID).
				Scan(&visibility)
			if errors.Is(err, sql.ErrNoRows) {
				return &channelspec.OperationError{Code: channelspec.ErrCodeDeclNotFound}
			}
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if visibility != "public" {
				return &sysopGateError{Status: 403, Code: "forbidden"}
			}
			return nil
		},
		Invoke: func(sys channelhost.SysOp, _ string) (channel.IntroduceResult, error) {
			return sys.Introduce(ctx, channelspec.IntroduceRequest{
				Ref: ref, DeclID: declID, InitiatorActorID: req.ActorID,
			})
		},
	})
	if err != nil {
		return channel.IntroduceResult{}, realmForwardError(err, ref)
	}
	return outcome.Value, nil
}

func (o realmOps) Remove(ctx context.Context, req realmtool.Requester, target actor.ActorID) (channel.RemoveResult, error) {
	if target == "" {
		return channel.RemoveResult{}, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "target required"}
	}
	ref := realmtool.DerivedRealmToolRef(req.ChannelID, req.RequestID)
	outcome, err := forwardSysop(ctx, o.app, req.ChannelID, sysopForward[channel.RemoveResult]{
		Predicate: func(bundle channelhost.Bundle) (channel.RemoveResult, bool, error) {
			facts, found, err := bundle.View().ActorFacts(ctx, target)
			return channel.RemoveResult{}, !found || !facts.Active, err
		},
		Qualify: func(bundle channelhost.Bundle) error {
			facts, found, err := bundle.View().ActorFacts(ctx, req.ActorID)
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if !found || !facts.Active {
				return &sysopGateError{Status: 403, Code: "forbidden"}
			}
			return nil
		},
		Invoke: func(sys channelhost.SysOp, _ string) (channel.RemoveResult, error) {
			return sys.Remove(ctx, channelspec.RemoveRequest{
				Ref: ref, Target: target, InitiatorActorID: req.ActorID,
			})
		},
	})
	if err != nil {
		return channel.RemoveResult{}, realmForwardError(err, ref)
	}
	return outcome.Value, nil
}

func realmForwardError(err error, ref string) error {
	var unknown *sysopUnknownError
	if errors.As(err, &unknown) {
		return &realmtool.ErrResultUnknown{Ref: ref}
	}
	var gate *sysopGateError
	if errors.As(err, &gate) {
		code := channelspec.RealmForbidden
		if gate.Status == 404 {
			code = channelspec.RealmChannelUnavailable
		}
		return &channelspec.RealmError{Code: code, Detail: gate.Code}
	}
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		return &channelspec.RealmError{
			Code: sysopRealmErrorCode(string(operationErr.Code)), Detail: string(operationErr.Code),
		}
	}
	return &realmtool.ErrResultUnknown{Ref: ref}
}

func (o realmOps) crossReader(ctx context.Context, req realmtool.Requester, source channel.ID) (channelhost.Bundle, channel.Reader, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	bundle, err := o.app.acquireBundle(ctx, source)
	if err != nil {
		return nil, channel.Reader{}, &channelspec.RealmError{Code: channelspec.RealmChannelUnavailable}
	}
	hasTool, err := bundle.View().HasDeclaredInstance(ctx, realmToolDeclID)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	if !hasTool {
		return nil, channel.Reader{}, &channelspec.RealmError{Code: channelspec.RealmCapabilityUnavailable, Detail: "source realm tool absent"}
	}
	if source == req.ChannelID {
		return bundle, channel.Reader{ActorID: req.ActorID, Mode: channel.ReaderMember}, nil
	}
	reader, reason, err := o.app.readerForPrincipal(ctx, bundle, facts.Principal, true)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	if reason != observeAllowed {
		return nil, channel.Reader{}, &channelspec.RealmError{Code: channelspec.RealmCapabilityUnavailable, Detail: string(reason)}
	}
	return bundle, reader, nil
}

func (o realmOps) ListResources(ctx context.Context, req realmtool.Requester, source channel.ID, q channel.ResourceListQuery) (channel.ResourcePage, error) {
	bundle, reader, err := o.crossReader(ctx, req, source)
	if err != nil {
		return channel.ResourcePage{}, err
	}
	return bundle.View().Resources().List(ctx, reader, q)
}

func (o realmOps) FetchResource(ctx context.Context, req realmtool.Requester, source channel.ID, id resource.ResourceID) (channel.ResourceFetch, error) {
	bundle, reader, err := o.crossReader(ctx, req, source)
	if err != nil {
		return channel.ResourceFetch{}, err
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
		return 0, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.body.Read(probe[:])
		if n > 0 {
			return 0, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		return n, &channelspec.RealmError{Code: channelspec.RealmInvalidRequest, Detail: "resource exceeds realm copy policy"}
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
		code := channelspec.RealmCapabilityUnavailable
		if reason == observeChannelAbsent {
			code = channelspec.RealmChannelUnavailable
		}
		return 0, &channelspec.RealmError{Code: code, Detail: string(reason)}
	}
	const chunk = 32 * 1024
	if len(p) > chunk {
		p = p[:chunk]
	}
	return r.body.Read(p)
}

func (r *observerResourceBody) Close() error { return r.body.Close() }
