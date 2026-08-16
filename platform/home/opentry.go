package home

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorctl"
)

// opEntry is a transport adapter. Actor lifecycle and identity mutations all
// enter Controller through actorSystem; this type owns no actor map, gate, or
// execution state. Business policy (owner protection, declaration visibility,
// placement host) is resolved HERE, before the typed command is issued.
type opEntry struct {
	home *Home
}

// removeRequest is membrane-private. Removal is only accepted as an operate
// frame; it is deliberately not part of the cross-package channel contract.
type removeRequest struct {
	Target           actor.ActorID
	InitiatorActorID actor.ActorID
}

var (
	_ sysactor.OperateExecutor = (*opEntry)(nil)
)

func (e *opEntry) available() error {
	if e == nil || e.home == nil || e.home.closed.Load() || e.home.actors == nil {
		return &channelspec.OperationError{
			Code: channelspec.ErrCodeChannelUnavailable, Detail: "channel is not serving", Retryable: true,
		}
	}
	return nil
}

func (e *opEntry) admit(ctx context.Context, principal string) (channel.AdmitResult, error) {
	if err := e.available(); err != nil {
		return channel.AdmitResult{}, err
	}
	if principal == "" {
		return channel.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "principal required",
		}
	}
	result, err := e.home.actors.Admit(ctx, actorctl.AdmitRequest{Principal: principal})
	if err == nil && result.ActorID != "" {
		e.home.ensureSubjectSlot(result.ActorID)
		e.home.narrateBirth(ctx, result.ActorID, actor.KindHuman, result.Created)
	}
	return result, err
}

// introduce is the one introduction path. Both doors — the out-of-band
// admission call above and the in-gate operate frame below — reach the same
// verdict, because the verdict reads the initiator's own facts instead of
// taking the door's word for what kind of initiator it is.
func (e *opEntry) introduce(
	ctx context.Context,
	declID string,
	principal string,
	initiator actor.ActorID,
	expected actor.Kind,
) (channel.IntroduceResult, error) {
	command, err := e.home.resolveIntroduction(ctx, declID, principal, initiator)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	if command.Kind != expected {
		return channel.IntroduceResult{}, &channelspec.OperationError{Code: channelspec.ErrCodeBadPayload, Detail: "kind does not match declaration class"}
	}
	result, err := e.home.actors.Introduce(ctx, command)
	if err == nil && result.ActorID != "" {
		e.home.narrateBirth(ctx, result.ActorID, command.Kind, result.Created)
	}
	return result, err
}

func (e *opEntry) remove(
	ctx context.Context,
	req removeRequest,
) (channel.RemoveResult, error) {
	if err := e.available(); err != nil {
		return channel.RemoveResult{}, err
	}
	if err := e.home.guardOwnerTerminal(ctx, req.Target); err != nil {
		return channel.RemoveResult{}, err
	}
	result, err := e.home.actors.Remove(ctx, actorctl.RemoveRequest{
		Target: req.Target, InitiatorActorID: req.InitiatorActorID,
	})
	return result, err
}

// Execute adapts the collaboration-plane system actor verbs.
func (e *opEntry) Execute(
	ctx context.Context,
	operation string,
	req sysactor.OperateRequest,
) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	switch operation {
	case sysactor.TypeIntroduceActor:
		var payload struct {
			Kind      actor.Kind `json:"kind"`
			DeclID    string     `json:"decl_id"`
			Principal string     `json:"principal"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "invalid introduce payload",
			}
		}
		switch payload.Kind {
		case actor.KindHuman:
			if payload.Principal == "" || payload.DeclID != "" {
				return nil, badIntroduce("human requires principal and forbids decl_id")
			}
			result, err := e.admit(ctx, payload.Principal)
			if err != nil {
				return nil, asOperateError(err)
			}
			return map[string]any{"instance_id": result.ActorID, "created": result.Created}, nil
		case actor.KindAgent, actor.KindTool:
			if payload.DeclID == "" || (payload.Kind == actor.KindTool && payload.Principal != "") {
				return nil, badIntroduce("agent/tool requires decl_id; tool forbids principal")
			}
			result, err := e.introduce(ctx, payload.DeclID, payload.Principal, req.Sender, payload.Kind)
			if err != nil {
				return nil, asOperateError(err)
			}
			return map[string]any{"instance_id": result.ActorID, "created": result.Created}, nil
		default:
			return nil, badIntroduce("kind must be human, agent, or tool")
		}

	case sysactor.TypeRemoveActor:
		var payload struct {
			InstanceID actor.ActorID `json:"instance_id"`
			DeclID     string        `json:"decl_id"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || (payload.InstanceID == "") == (payload.DeclID == "") {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "exactly one of instance_id or decl_id required",
			}
		}
		if payload.DeclID != "" {
			ids, err := e.home.View().DeclaredInstances(ctx, payload.DeclID)
			if err != nil {
				return nil, asOperateError(err)
			}
			if len(ids) == 0 {
				return map[string]any{"removed": false}, nil
			}
			if len(ids) != 1 {
				return nil, &sysactor.OperateError{Code: string(channelspec.ErrCodeBadPayload), Detail: "declaration instance is ambiguous"}
			}
			payload.InstanceID = ids[0]
		}
		result, err := e.remove(ctx, removeRequest{
			Target: payload.InstanceID, InitiatorActorID: req.Sender,
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"removed": result.Removed}, nil

	case sysactor.TypeRestartActor:
		var payload struct {
			InstanceID actor.ActorID `json:"instance_id"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || payload.InstanceID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "instance_id required",
			}
		}
		if err := e.home.guardOwnerTerminal(ctx, payload.InstanceID); err != nil {
			return nil, asOperateError(err)
		}
		if err := e.home.actors.Restart(ctx, actorctl.RestartRequest{
			ActorID: payload.InstanceID,
		}); err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"restarted": payload.InstanceID}, nil

	default:
		return nil, &sysactor.OperateError{
			Code: string(channelspec.ErrCodeNotAcceptedSource), Detail: "operation is not accepted",
		}
	}
}

func badIntroduce(detail string) *sysactor.OperateError {
	return &sysactor.OperateError{Code: string(channelspec.ErrCodeBadPayload), Detail: detail}
}

func decodeStrict(raw json.RawMessage, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

// narrateBirth writes the "joined the channel" narration for a freshly created
// record. A replayed birth (created=false) narrates nothing. The narration is
// composed from the command's own inputs — the tail never reads truth back.
func (h *Home) narrateBirth(ctx context.Context, id actor.ActorID, kind actor.Kind, created bool) {
	if !created {
		return
	}
	h.announceRegistered(ctx, id, kind)
}

func asOperateError(err error) error {
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		return &sysactor.OperateError{
			Code: string(operationErr.Code), Detail: operationErr.Detail,
		}
	}
	switch {
	case errors.Is(err, actorctl.ErrInvalidMutation):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeBadPayload), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrInactive), errors.Is(err, actorctl.ErrStaleAttempt):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeMemberInactive), Detail: err.Error(),
		}
	case errors.Is(err, actorctl.ErrClosed), errors.Is(err, actorctl.ErrChannelClosing):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeChannelUnavailable), Detail: err.Error(),
		}
	default:
		return err
	}
}
