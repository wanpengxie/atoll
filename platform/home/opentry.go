package home

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// opEntry is a transport adapter. Actor lifecycle and identity mutations all
// enter ChannelActors; this type owns no actor map, gate, or execution state.
type opEntry struct {
	home *Home
}

var (
	_ sysactor.OperateExecutor = (*opEntry)(nil)
	_ sysactor.SystemOps       = (*opEntry)(nil)
)

func (e *opEntry) available() error {
	if e == nil || e.home == nil || e.home.closed.Load() || e.home.actors == nil {
		return &channel.OperationError{
			Code: channel.ErrCodeChannelUnavailable, Detail: "channel is not serving", Retryable: true,
		}
	}
	return nil
}

func systemMeta(ref string, request any) (storespec.SysOpMeta, error) {
	if strings.TrimSpace(ref) == "" {
		return storespec.SysOpMeta{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "ref required",
		}
	}
	digest, err := channel.Digest(request)
	if err != nil {
		return storespec.SysOpMeta{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: err.Error(),
		}
	}
	return storespec.SysOpMeta{
		Anchor: channel.RefCorrelation(ref), RequestDigest: digest,
		Source: storespec.SysOpSourceSystem, Sender: actor.SystemActorID,
	}, nil
}

func memberMeta(anchor string, sender actor.ActorID, request any) (storespec.SysOpMeta, error) {
	if strings.TrimSpace(anchor) == "" || sender == "" {
		return storespec.SysOpMeta{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "request id and sender required",
		}
	}
	digest, err := channel.Digest(request)
	if err != nil {
		return storespec.SysOpMeta{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: err.Error(),
		}
	}
	return storespec.SysOpMeta{
		Anchor: channel.MessageCorrelation(anchor), RequestDigest: digest,
		Source: storespec.SysOpSourceMember, Sender: sender,
	}, nil
}

func actorCommandMeta(
	ref string,
	member *actorctl.MemberOperation,
	systemRequest any,
) (storespec.SysOpMeta, error) {
	if member == nil {
		return systemMeta(ref, systemRequest)
	}
	return memberMeta(string(member.RequestID), member.Sender, member.Payload)
}

func (e *opEntry) Admit(ctx context.Context, req channel.AdmitRequest) (channel.AdmitResult, error) {
	if err := e.available(); err != nil {
		return channel.AdmitResult{}, err
	}
	result, err := e.home.actors.Admit(ctx, actorctl.AdmitRequest{
		Ref: req.Ref, Principal: req.Principal,
	})
	if err == nil && result.ActorID != "" {
		e.home.ensureSubjectSlot(result.ActorID)
	}
	return result, err
}

func (e *opEntry) Introduce(
	ctx context.Context,
	req channel.IntroduceRequest,
) (channel.IntroduceResult, error) {
	if err := e.available(); err != nil {
		return channel.IntroduceResult{}, err
	}
	return e.home.actors.Introduce(ctx, actorctl.IntroduceRequest{
		Ref: req.Ref, DeclID: req.DeclID, InitiatorActorID: req.InitiatorActorID,
	})
}

func (e *opEntry) Remove(
	ctx context.Context,
	req channel.RemoveRequest,
) (channel.RemoveResult, error) {
	if err := e.available(); err != nil {
		return channel.RemoveResult{}, err
	}
	return e.home.actors.Remove(ctx, actorctl.RemoveRequest{
		Ref: req.Ref, Target: req.Target, InitiatorActorID: req.InitiatorActorID,
	})
}

func (e *opEntry) AttachDaemon(
	ctx context.Context,
	req channel.DaemonRequest,
) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	return e.home.actors.AttachDaemon(ctx, req)
}

func (e *opEntry) DetachDaemon(
	ctx context.Context,
	req channel.DaemonRequest,
) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	return e.home.actors.DetachDaemon(ctx, req)
}

// Execute adapts the collaboration-plane system actor verbs. RequestID and
// sender remain collaboration identity; no AttemptKey is added to these
// operation values.
func (e *opEntry) Execute(
	ctx context.Context,
	operation string,
	req sysactor.OperateRequest,
) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	member := func() *actorctl.MemberOperation {
		return &actorctl.MemberOperation{
			RequestID: message.ID(req.Anchor), Sender: req.Sender,
			Payload: append(json.RawMessage(nil), req.Payload...),
		}
	}
	switch operation {
	case sysactor.TypeIntroduceActor:
		var payload struct {
			DeclID string `json:"decl_id"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.DeclID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: "decl_id required",
			}
		}
		result, err := e.home.actors.Introduce(ctx, actorctl.IntroduceRequest{
			DeclID: payload.DeclID, InitiatorActorID: req.Sender, Member: member(),
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"instance_id": result.ActorID, "created": result.Created}, nil

	case sysactor.TypeRemoveActor:
		var payload struct {
			InstanceID actor.ActorID `json:"instance_id"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required",
			}
		}
		result, err := e.home.actors.Remove(ctx, actorctl.RemoveRequest{
			Target: payload.InstanceID, InitiatorActorID: req.Sender, Member: member(),
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"removed": result.Removed}, nil

	case sysactor.TypeRestartActor:
		var payload struct {
			InstanceID actor.ActorID `json:"instance_id"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required",
			}
		}
		err := e.home.actors.Restart(ctx, actorctl.RestartRequest{
			ActorID: payload.InstanceID, CallerActorID: req.Sender,
			RequestID: message.ID(req.Anchor),
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"restarted": payload.InstanceID}, nil

	case sysactor.TypeSetDefaultAgent:
		var payload struct {
			InstanceID actor.ActorID `json:"instance_id"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: err.Error(),
			}
		}
		meta, err := memberMeta(req.Anchor, req.Sender, json.RawMessage(req.Payload))
		if err != nil {
			return nil, err
		}
		result, err := e.home.cs.SysOps.SetDefaultAgent(ctx, storespec.SetDefaultTx{
			SysOpMeta: meta, Target: payload.InstanceID,
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		homeActorEffects{home: e.home}.ApplyPostCommit(result.Effects)
		return map[string]any{"default_agent": payload.InstanceID}, nil

	default:
		return nil, &sysactor.OperateError{
			Code: string(channel.ErrCodeNotAcceptedSource), Detail: "operation is not accepted",
		}
	}
}

func asOperateError(err error) error {
	var operationErr *channel.OperationError
	if errors.As(err, &operationErr) {
		return &sysactor.OperateError{
			Code: string(operationErr.Code), Detail: operationErr.Detail,
		}
	}
	switch {
	case errors.Is(err, actorctl.ErrInactive), errors.Is(err, actorctl.ErrStaleAttempt):
		return &sysactor.OperateError{
			Code: string(channel.ErrCodeMemberInactive), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrClosed), errors.Is(err, actorctl.ErrChannelClosing):
		return &sysactor.OperateError{
			Code: string(channel.ErrCodeChannelUnavailable), Detail: err.Error(),
		}
	default:
		return err
	}
}

// SystemOps is the assembly-only direct realm adapter.
func SystemOps(h *Home) sysactor.SystemOps {
	if h == nil {
		return nil
	}
	return h.opEntry
}
