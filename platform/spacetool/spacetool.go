package spacetool

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

const (
	TypeListDeclarations   = "space.declarations.list"
	TypeInspectDeclaration = "space.declarations.inspect"
	TypeCreateDeclaration  = "space.declarations.create"
	TypeEditDeclaration    = "space.declarations.edit"
	TypeRevokeDeclaration  = "space.declarations.revoke"
	TypeIntroduce          = "space.introduce"
	TypeRemove             = "space.remove"
	TypeListResources      = "space.resources.list"
	TypeFetchResource      = "space.resources.fetch"
)

type SpaceOps interface {
	ListDeclarations(context.Context, Requester) ([]DeclSummary, error)
	InspectDeclaration(context.Context, Requester, string) (DeclDetail, error)
	CreateDeclaration(context.Context, Requester, DeclSpec) (DeclDetail, error)
	EditDeclaration(context.Context, Requester, string, DeclSpec) (DeclDetail, error)
	RevokeDeclaration(context.Context, Requester, string) error
	Introduce(context.Context, Requester, string, IntroduceOpts) (channel.IntroduceResult, error)
	Remove(context.Context, Requester, actor.ActorID) (channel.RemoveResult, error)
	ListResources(context.Context, Requester, channel.ID, channel.ResourceListQuery) (channel.ResourcePage, error)
	FetchResource(context.Context, Requester, channel.ID, resource.ResourceID) (channel.ResourceFetch, error)
}

func Def(ops SpaceOps) actorbase.Def {
	return actorbase.Def{Doc: "space boundary operations", New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { return serve(sys, ops) }, nil
	}}
}

func serve(sys actorbase.Sys, ops SpaceOps) error {
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

func requester(msg actorbase.Msg) Requester {
	return Requester{ActorID: msg.Sender.ID, ChannelID: msg.ChannelID, RequestID: string(msg.ID)}
}

func decode(msg actorbase.Msg, out any) error {
	if len(msg.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(msg.Payload, out); err != nil {
		return &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "invalid JSON payload"}
	}
	return nil
}

func fail(sys actorbase.Sys, msg actorbase.Msg, err error) {
	var spaceErr *channelspec.SpaceError
	if errors.As(err, &spaceErr) {
		_, _ = sys.Fail(msg, string(spaceErr.Code), spaceErr.Detail)
		return
	}
	var unknown *ErrResultUnknown
	if errors.As(err, &unknown) {
		_, _ = sys.Reply(msg, map[string]any{"status": "result_unknown", "ref": unknown.Ref})
		return
	}
	_, _ = sys.Fail(msg, string(channelspec.SpaceUnavailable), err.Error())
}

func handle(sys actorbase.Sys, ops SpaceOps, msg actorbase.Msg) {
	if ops == nil {
		fail(sys, msg, &channelspec.SpaceError{Code: channelspec.SpaceUnavailable})
		return
	}
	req := requester(msg)
	var result any
	var err error
	switch msg.Type {
	case TypeListDeclarations:
		var declarations []DeclSummary
		declarations, err = ops.ListDeclarations(msg.Ctx(), req)
		result = map[string]any{"declarations": declarations}
	case TypeInspectDeclaration:
		var p struct {
			DeclID string `json:"decl_id"`
		}
		if err = decode(msg, &p); err == nil && p.DeclID != "" {
			var declaration DeclDetail
			declaration, err = ops.InspectDeclaration(msg.Ctx(), req, p.DeclID)
			result = map[string]any{"declaration": declaration}
		} else if err == nil {
			err = &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "decl_id required"}
		}
	case TypeCreateDeclaration:
		var p DeclSpec
		if err = decode(msg, &p); err == nil {
			var declaration DeclDetail
			declaration, err = ops.CreateDeclaration(msg.Ctx(), req, p)
			result = map[string]any{"declaration": declaration}
		}
	case TypeEditDeclaration:
		var p struct {
			DeclID string   `json:"decl_id"`
			Spec   DeclSpec `json:"spec"`
		}
		if err = decode(msg, &p); err == nil {
			var declaration DeclDetail
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
			result, err = ops.Introduce(msg.Ctx(), req, p.DeclID, IntroduceOpts{})
		}
	case TypeRemove:
		var p struct {
			Target actor.ActorID `json:"target"`
		}
		if err = decode(msg, &p); err == nil {
			result, err = ops.Remove(msg.Ctx(), req, p.Target)
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
				body, err = io.ReadAll(fetched.Body)
				if err == nil {
					newID := resource.ResourceID("space-copy:" + uuid.NewString())
					out, createErr := sys.Resource().CreateFrom(newID, body, p)
					if createErr != nil || !out.Accepted() {
						if createErr != nil {
							err = createErr
						} else {
							err = &channelspec.SpaceError{Code: channelspec.SpaceConflict, Detail: string(out.RejectReason)}
						}
					} else {
						result = map[string]any{"resource_id": newID, "source": p}
					}
				}
			}
		}
	default:
		err = &channelspec.SpaceError{Code: channelspec.SpaceInvalidRequest, Detail: "unsupported space operation"}
	}
	if err != nil {
		fail(sys, msg, err)
		return
	}
	_, _ = sys.Reply(msg, result)
}
