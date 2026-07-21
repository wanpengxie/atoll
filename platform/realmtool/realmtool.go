package realmtool

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

const (
	TypeListDeclarations   = "realm.declarations.list"
	TypeInspectDeclaration = "realm.declarations.inspect"
	TypeCreateDeclaration  = "realm.declarations.create"
	TypeEditDeclaration    = "realm.declarations.edit"
	TypeRevokeDeclaration  = "realm.declarations.revoke"
	TypeIntroduce          = "realm.introduce"
	TypeRemove             = "realm.remove"
	TypeOperationStatus    = "realm.operation_status"
	TypeListResources      = "realm.resources.list"
	TypeFetchResource      = "realm.resources.fetch"
)

type RealmOps interface {
	ListDeclarations(context.Context, channel.Requester) ([]channel.DeclSummary, error)
	InspectDeclaration(context.Context, channel.Requester, string) (channel.DeclDetail, error)
	CreateDeclaration(context.Context, channel.Requester, channel.DeclSpec) (channel.DeclDetail, error)
	EditDeclaration(context.Context, channel.Requester, string, channel.DeclSpec) (channel.DeclDetail, error)
	RevokeDeclaration(context.Context, channel.Requester, string) error
	Introduce(context.Context, channel.Requester, string, channel.IntroduceOpts) (channel.IntroduceResult, error)
	Remove(context.Context, channel.Requester, actor.ActorID) (channel.RemoveResult, error)
	OperationStatus(context.Context, channel.Requester, string) (channel.OperationView, error)
	ListResources(context.Context, channel.Requester, channel.ID, channel.ResourceListQuery) (channel.ResourcePage, error)
	FetchResource(context.Context, channel.Requester, channel.ID, resource.ResourceID) (channel.ResourceFetch, error)
}

func Def(ops RealmOps) actorbase.Def {
	return actorbase.Def{Doc: "realm boundary operations", New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { return serve(sys, ops) }, nil
	}}
}

func serve(sys actorbase.Sys, ops RealmOps) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return nil
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		handle(sys, ops, msg)
	}
}

func requester(msg actorbase.Msg) channel.Requester {
	return channel.Requester{ActorID: msg.Sender.ID, ChannelID: msg.ChannelID, RequestID: string(msg.ID)}
}

func decode(msg actorbase.Msg, out any) error {
	if len(msg.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(msg.Payload, out); err != nil {
		return &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "invalid JSON payload"}
	}
	return nil
}

func fail(sys actorbase.Sys, msg actorbase.Msg, err error) {
	var realmErr *channel.RealmError
	if errors.As(err, &realmErr) {
		_, _ = sys.Fail(msg, string(realmErr.Code), realmErr.Detail)
		return
	}
	var unknown *channel.ErrResultUnknown
	if errors.As(err, &unknown) {
		_, _ = sys.Reply(msg, map[string]any{"status": "result_unknown", "ref": unknown.Ref})
		return
	}
	_, _ = sys.Fail(msg, string(channel.RealmUnavailable), err.Error())
}

func handle(sys actorbase.Sys, ops RealmOps, msg actorbase.Msg) {
	if ops == nil {
		fail(sys, msg, &channel.RealmError{Code: channel.RealmUnavailable})
		return
	}
	req := requester(msg)
	var result any
	var err error
	switch msg.Type {
	case TypeListDeclarations:
		var declarations []channel.DeclSummary
		declarations, err = ops.ListDeclarations(msg.Ctx(), req)
		result = map[string]any{"declarations": declarations}
	case TypeInspectDeclaration:
		var p struct {
			DeclID string `json:"decl_id"`
		}
		if err = decode(msg, &p); err == nil && p.DeclID != "" {
			var declaration channel.DeclDetail
			declaration, err = ops.InspectDeclaration(msg.Ctx(), req, p.DeclID)
			result = map[string]any{"declaration": declaration}
		} else if err == nil {
			err = &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "decl_id required"}
		}
	case TypeCreateDeclaration:
		var p channel.DeclSpec
		if err = decode(msg, &p); err == nil {
			var declaration channel.DeclDetail
			declaration, err = ops.CreateDeclaration(msg.Ctx(), req, p)
			result = map[string]any{"declaration": declaration}
		}
	case TypeEditDeclaration:
		var p struct {
			DeclID string           `json:"decl_id"`
			Spec   channel.DeclSpec `json:"spec"`
		}
		if err = decode(msg, &p); err == nil {
			var declaration channel.DeclDetail
			declaration, err = ops.EditDeclaration(msg.Ctx(), req, p.DeclID, p.Spec)
			result = map[string]any{"declaration": declaration}
		}
	case TypeRevokeDeclaration:
		var p struct {
			DeclID string `json:"decl_id"`
		}
		if err = decode(msg, &p); err == nil {
			err = ops.RevokeDeclaration(msg.Ctx(), req, p.DeclID)
			result = map[string]any{"revoked": p.DeclID}
		}
	case TypeIntroduce:
		var p struct {
			DeclID string `json:"decl_id"`
		}
		if err = decode(msg, &p); err == nil {
			result, err = ops.Introduce(msg.Ctx(), req, p.DeclID, channel.IntroduceOpts{})
		}
	case TypeRemove:
		var p struct {
			Target actor.ActorID `json:"target"`
		}
		if err = decode(msg, &p); err == nil {
			result, err = ops.Remove(msg.Ctx(), req, p.Target)
		}
	case TypeOperationStatus:
		var p struct {
			Ref string `json:"ref"`
		}
		if err = decode(msg, &p); err == nil {
			result, err = ops.OperationStatus(msg.Ctx(), req, p.Ref)
		}
	case TypeListResources:
		var p struct {
			ChannelID channel.ID                `json:"channel_id"`
			Query     channel.ResourceListQuery `json:"query"`
		}
		if err = decode(msg, &p); err == nil {
			result, err = ops.ListResources(msg.Ctx(), req, p.ChannelID, p.Query)
		}
	case TypeFetchResource:
		var p channel.ResourceRef
		if err = decode(msg, &p); err == nil {
			var fetched channel.ResourceFetch
			fetched, err = ops.FetchResource(msg.Ctx(), req, p.ChannelID, p.ResourceID)
			if err == nil {
				defer fetched.Body.Close()
				var body []byte
				body, err = io.ReadAll(io.LimitReader(fetched.Body, (32<<20)+1))
				if err == nil && len(body) > 32<<20 {
					err = &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "resource exceeds realm copy limit"}
				}
				if err == nil {
					newID := resource.ResourceID("realm-copy:" + uuid.NewString())
					out, createErr := sys.Resource().CreateFrom(newID, body, p)
					if createErr != nil || !out.Accepted() {
						if createErr != nil {
							err = createErr
						} else {
							err = &channel.RealmError{Code: channel.RealmConflict, Detail: string(out.RejectReason)}
						}
					} else {
						result = map[string]any{"resource_id": newID, "source": p}
					}
				}
			}
		}
	default:
		err = &channel.RealmError{Code: channel.RealmInvalidRequest, Detail: "unsupported realm operation"}
	}
	if err != nil {
		fail(sys, msg, err)
		return
	}
	_, _ = sys.Reply(msg, result)
}
