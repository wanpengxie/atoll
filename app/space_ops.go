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
	"github.com/wanpengxie/atoll/platform/spacetool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

type spaceOps struct{ app *App }

// spaceResourceCopyLimitBytes is reference-space policy. The membrane and its
// space-tool cell do not invent transport limits; they consume the stream and
// propagate this space-owned rejection unchanged.
const spaceResourceCopyLimitBytes int64 = 32 << 20

func (o spaceOps) requesterFacts(ctx context.Context, req spacetool.Requester) (channelspec.ActorFacts, error) {
	if req.ActorID == "" || req.ChannelID == "" || req.RequestID == "" {
		return channelspec.ActorFacts{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "incomplete requester"}
	}
	bundle, ok := o.app.host.Acquire(req.ChannelID)
	if !ok {
		return channelspec.ActorFacts{}, &channelspec.SpaceError{Code: channelspec.SpaceChannelUnavailable}
	}
	facts, found, err := bundle.View().ActorFacts(ctx, req.ActorID)
	if err != nil {
		return channelspec.ActorFacts{}, err
	}
	if !found || !facts.Active {
		return channelspec.ActorFacts{}, &channelspec.SpaceError{Code: channelspec.SpaceForbidden, Detail: "requester is not active"}
	}
	return facts, nil
}

func scanDecl(row interface{ Scan(...any) error }) (spacetool.DeclDetail, error) {
	var d spacetool.DeclDetail
	var config sql.NullString
	err := row.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &config, &d.Visibility)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if config.Valid && config.String != "" {
		d.Config = json.RawMessage(config.String)
	}
	return d, nil
}

func (o spaceOps) ListDeclarations(ctx context.Context, req spacetool.Requester) ([]spacetool.DeclSummary, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return nil, err
	}
	rows, err := o.app.db.QueryContext(ctx, `SELECT id,name,owner,default_class,visibility FROM actor_decls WHERE deleted_at IS NULL AND (visibility='public' OR owner=?) ORDER BY created_at,id`, facts.Principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []spacetool.DeclSummary{}
	for rows.Next() {
		var d spacetool.DeclSummary
		if err := rows.Scan(&d.ID, &d.Name, &d.Owner, &d.Class, &d.Visibility); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (o spaceOps) InspectDeclaration(ctx context.Context, req spacetool.Requester, declID string) (spacetool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	d, err := scanDecl(o.app.db.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,visibility FROM actor_decls WHERE id=? AND deleted_at IS NULL`, declID))
	if errors.Is(err, sql.ErrNoRows) {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceDeclNotFound}
	}
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if !declarationVisibleTo(d.Visibility, d.Owner, facts.Principal) {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceForbidden}
	}
	return d, nil
}

func normalizeDeclSpec(spec spacetool.DeclSpec) (spacetool.DeclSpec, error) {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.Class = strings.TrimSpace(spec.Class)
	if spec.Name == "" || spec.Class == "" {
		return spec, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "name and class required"}
	}
	if spec.Visibility == "" {
		spec.Visibility = "private"
	}
	if spec.Visibility != "public" && spec.Visibility != "private" {
		return spec, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "invalid visibility"}
	}
	if len(spec.Config) > 0 && !isJSONObject(spec.Config) {
		return spec, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "config must be an object"}
	}
	return spec, nil
}

func (o spaceOps) CreateDeclaration(ctx context.Context, req spacetool.Requester, spec spacetool.DeclSpec) (spacetool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceForbidden, Detail: "only humans may write the space declaration registry"}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if _, ok, err := o.app.declarationClassKind(ctx, spec.Class); err != nil || !ok {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "unknown or reserved class"}
	}
	if err := registry.ValidateConfig(spec.Class, spec.Config); err != nil {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "config_invalid"}
	}
	id := uuid.NewString()
	now := time.Now().UnixMilli()
	var config any
	if len(spec.Config) > 0 {
		config = string(spec.Config)
	}
	_, err = o.app.db.ExecContext(ctx, `INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`, id, spec.Name, facts.Principal, spec.Class, config, now, now, spec.Visibility)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	return spacetool.DeclDetail{DeclSummary: spacetool.DeclSummary{ID: id, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o spaceOps) EditDeclaration(ctx context.Context, req spacetool.Requester, declID string, spec spacetool.DeclSpec) (spacetool.DeclDetail, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if facts.Kind != actor.KindHuman {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceForbidden}
	}
	spec, err = normalizeDeclSpec(spec)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if _, ok, err := o.app.declarationClassKind(ctx, spec.Class); err != nil || !ok {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "unknown or reserved class"}
	}
	if err := registry.ValidateConfig(spec.Class, spec.Config); err != nil {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "config_invalid"}
	}
	tx, err := o.app.db.BeginTx(ctx, nil)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	defer tx.Rollback()
	var currentClass string
	if err := tx.QueryRowContext(ctx, `SELECT default_class FROM actor_decls WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, declID, facts.Principal).Scan(&currentClass); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceDeclNotFound}
		}
		return spacetool.DeclDetail{}, err
	}
	sameKind, err := o.app.declarationClassTransition(ctx, currentClass, spec.Class)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if !sameKind {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "class must remain within the declaration kind"}
	}
	now := time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `UPDATE actor_decls SET name=?,default_class=?,config_json=?,visibility=?,updated_at=? WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, spec.Name, spec.Class, string(spec.Config), spec.Visibility, now, declID, facts.Principal)
	if err != nil {
		return spacetool.DeclDetail{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return spacetool.DeclDetail{}, &channelspec.SpaceError{Code: channelspec.SpaceDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return spacetool.DeclDetail{}, err
	}
	o.app.pokeAllChannels(ctx)
	return spacetool.DeclDetail{DeclSummary: spacetool.DeclSummary{ID: declID, Name: spec.Name, Owner: facts.Principal, Visibility: spec.Visibility, Class: spec.Class}, Config: spec.Config}, nil
}

func (o spaceOps) RevokeDeclaration(ctx context.Context, req spacetool.Requester, declID string) error {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return err
	}
	if facts.Kind != actor.KindHuman {
		return &channelspec.SpaceError{Code: channelspec.SpaceForbidden}
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
		return &channelspec.SpaceError{Code: channelspec.SpaceDeclNotFound}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (o spaceOps) Introduce(ctx context.Context, req spacetool.Requester, declID string, _ spacetool.IntroduceOpts) (channel.IntroduceResult, error) {
	if req.ActorID == "" || req.ChannelID == "" || req.RequestID == "" {
		return channel.IntroduceResult{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "incomplete requester"}
	}
	ref := spacetool.DerivedSpaceToolRef(req.ChannelID, req.RequestID)
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
		return channel.IntroduceResult{}, spaceForwardError(err, ref)
	}
	return outcome.Value, nil
}

func (o spaceOps) Remove(ctx context.Context, req spacetool.Requester, target actor.ActorID) (channel.RemoveResult, error) {
	if req.ActorID == "" || req.ChannelID == "" || req.RequestID == "" {
		return channel.RemoveResult{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "incomplete requester"}
	}
	if target == "" {
		return channel.RemoveResult{}, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "target required"}
	}
	ref := spacetool.DerivedSpaceToolRef(req.ChannelID, req.RequestID)
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
		return channel.RemoveResult{}, spaceForwardError(err, ref)
	}
	return outcome.Value, nil
}

func spaceForwardError(err error, ref string) error {
	var unknown *sysopUnknownError
	if errors.As(err, &unknown) {
		return &spacetool.ErrResultUnknown{Ref: ref}
	}
	var gate *sysopGateError
	if errors.As(err, &gate) {
		code := channelspec.SpaceForbidden
		if gate.Status == 404 {
			code = channelspec.SpaceChannelUnavailable
		}
		return &channelspec.SpaceError{Code: code, Detail: gate.Code}
	}
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		return &channelspec.SpaceError{
			Code: sysopSpaceErrorCode(string(operationErr.Code)), Detail: string(operationErr.Code),
		}
	}
	return &spacetool.ErrResultUnknown{Ref: ref}
}

func (o spaceOps) crossReader(ctx context.Context, req spacetool.Requester, source channel.ID) (channelhost.Bundle, channel.Reader, error) {
	facts, err := o.requesterFacts(ctx, req)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	bundle, err := o.app.acquireBundle(ctx, source)
	if err != nil {
		return nil, channel.Reader{}, &channelspec.SpaceError{Code: channelspec.SpaceChannelUnavailable}
	}
	hasTool, err := bundle.View().HasDeclaredInstance(ctx, spaceToolDeclID)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	if !hasTool {
		return nil, channel.Reader{}, &channelspec.SpaceError{Code: channelspec.SpaceCapabilityUnavailable, Detail: "source space tool absent"}
	}
	if source == req.ChannelID {
		return bundle, channel.Reader{ActorID: req.ActorID, Mode: channel.ReaderMember}, nil
	}
	reader, reason, err := o.app.readerForPrincipal(ctx, bundle, facts.Principal, true)
	if err != nil {
		return nil, channel.Reader{}, err
	}
	if reason != observeAllowed {
		return nil, channel.Reader{}, &channelspec.SpaceError{Code: channelspec.SpaceCapabilityUnavailable, Detail: string(reason)}
	}
	return bundle, reader, nil
}

func (o spaceOps) ListResources(ctx context.Context, req spacetool.Requester, source channel.ID, q channel.ResourceListQuery) (channel.ResourcePage, error) {
	bundle, reader, err := o.crossReader(ctx, req, source)
	if err != nil {
		return channel.ResourcePage{}, err
	}
	return bundle.View().Resources().List(ctx, reader, q)
}

func (o spaceOps) FetchResource(ctx context.Context, req spacetool.Requester, source channel.ID, id resource.ResourceID) (channel.ResourceFetch, error) {
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
	fetched.Body = newSpaceCopyPolicyBody(fetched.Body, spaceResourceCopyLimitBytes)
	return fetched, nil
}

type spaceCopyPolicyBody struct {
	body      io.ReadCloser
	remaining int64
}

func newSpaceCopyPolicyBody(body io.ReadCloser, limit int64) io.ReadCloser {
	return &spaceCopyPolicyBody{body: body, remaining: limit}
}

func (r *spaceCopyPolicyBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining < 0 {
		return 0, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "resource exceeds space copy policy"}
	}
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.body.Read(probe[:])
		if n > 0 {
			return 0, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "resource exceeds space copy policy"}
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		return n, &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "resource exceeds space copy policy"}
	}
	return n, err
}

func (r *spaceCopyPolicyBody) Close() error { return r.body.Close() }

// observerResourceBody binds a cross-membrane byte stream to its space gate.
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
		code := channelspec.SpaceCapabilityUnavailable
		if reason == observeChannelAbsent {
			code = channelspec.SpaceChannelUnavailable
		}
		return 0, &channelspec.SpaceError{Code: code, Detail: string(reason)}
	}
	const chunk = 32 * 1024
	if len(p) > chunk {
		p = p[:chunk]
	}
	return r.body.Read(p)
}

func (r *observerResourceBody) Close() error { return r.body.Close() }
