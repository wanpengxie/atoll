package home

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
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

func (e *opEntry) admit(ctx context.Context, principal string) (actorctl.AdmitResult, error) {
	if err := e.available(); err != nil {
		return actorctl.AdmitResult{}, err
	}
	if principal == "" {
		return actorctl.AdmitResult{}, &channelspec.OperationError{
			Code: channelspec.ErrCodeBadPayload, Detail: "principal required",
		}
	}
	result, err := e.home.actors.Admit(ctx, actorctl.AdmitRequest{Principal: principal})
	if err == nil && result.ActorID != "" {
		e.home.ensureSubjectSlot(result.ActorID)
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
	initiator actor.ActorID,
) (actorctl.IntroduceResult, error) {
	command, err := e.home.resolveIntroduction(ctx, declID, initiator)
	if err != nil {
		return actorctl.IntroduceResult{}, err
	}
	result, err := e.home.actors.Introduce(ctx, command)
	return result, err
}

func (e *opEntry) remove(
	ctx context.Context,
	req removeRequest,
) (actorctl.RemoveResult, error) {
	if err := e.available(); err != nil {
		return actorctl.RemoveResult{}, err
	}
	if err := e.home.guardOwnerTerminal(ctx, req.Target); err != nil {
		return actorctl.RemoveResult{}, err
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
	case sysactor.TypeMemberCreate:
		var payload struct {
			DeclID string `json:"decl_id"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || payload.DeclID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "decl_id required",
			}
		}
		result, err := e.introduce(ctx, payload.DeclID, req.Caller.Actor)
		if err != nil {
			return nil, asOperateError(err)
		}
		by := map[string]any{"caller": req.Caller}
		if facts, active, factsErr := e.home.actors.ActorFacts(ctx, req.Caller.Actor); factsErr == nil && active && facts.SourceDeclID == payload.DeclID {
			by = map[string]any{"fork_of": req.Caller.Actor}
		}
		e.home.narrateBirth(ctx, result.ActorID, result.Created, map[string]any{
			"decl_id": payload.DeclID, "by": by,
		})
		return map[string]any{"member": result.ActorID}, nil

	case sysactor.TypeMemberAdmit:
		var payload struct {
			Principal string `json:"principal"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || payload.Principal == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "principal required",
			}
		}
		result, err := e.admit(ctx, payload.Principal)
		if err != nil {
			return nil, asOperateError(err)
		}
		e.home.narrateBirth(ctx, result.ActorID, result.Created, map[string]any{
			"principal": payload.Principal, "by": map[string]any{"caller": req.Caller},
		})
		return map[string]any{"member": result.ActorID}, nil

	case sysactor.TypeMemberDelete:
		var payload struct {
			Member actor.ActorID `json:"member"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || payload.Member == "" {
			return nil, &sysactor.OperateError{Code: string(channelspec.ErrCodeBadPayload), Detail: "member required"}
		}
		resolved, err := e.home.actors.ResolveTarget(string(payload.Member))
		if err != nil {
			return nil, asOperateError(err)
		}
		result, err := e.remove(ctx, removeRequest{
			Target: resolved, InitiatorActorID: req.Caller.Actor,
		})
		if err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"removed": result.Removed}, nil

	case sysactor.TypeMemberRestart:
		var payload struct {
			Member actor.ActorID `json:"member"`
		}
		if err := decodeStrict(req.Payload, &payload); err != nil || payload.Member == "" {
			return nil, &sysactor.OperateError{
				Code: string(channelspec.ErrCodeBadPayload), Detail: "member required",
			}
		}
		resolved, err := e.home.actors.ResolveTarget(string(payload.Member))
		if err != nil {
			return nil, asOperateError(err)
		}
		payload.Member = resolved
		if err := e.home.guardOwnerTerminal(ctx, payload.Member); err != nil {
			return nil, asOperateError(err)
		}
		if err := e.home.actors.Restart(ctx, actorctl.RestartRequest{
			ActorID: payload.Member,
		}); err != nil {
			return nil, asOperateError(err)
		}
		return map[string]any{"member": payload.Member}, nil

	default:
		return nil, &sysactor.OperateError{
			Code: string(channelspec.ErrCodeNotAcceptedSource), Detail: "operation is not accepted",
		}
	}
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
func (h *Home) narrateBirth(ctx context.Context, id actor.ActorID, created bool, fields map[string]any) {
	if !created {
		return
	}
	h.announceRegistered(ctx, id, fields)
}

func asOperateError(err error) error {
	var operationErr *channelspec.OperationError
	if errors.As(err, &operationErr) {
		return &sysactor.OperateError{
			Code: string(operationErr.Code), Detail: operationErr.Detail,
		}
	}
	var targetErr *actorbase.TargetResolveError
	if errors.As(err, &targetErr) {
		return &sysactor.OperateError{Code: targetErr.Code, Detail: targetErr.Error()}
	}
	switch {
	case errors.Is(err, storespec.ErrConflictExists):
		return &sysactor.OperateError{
			Code: string(channelspec.ErrCodeConflictExists), Detail: err.Error(),
		}
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
