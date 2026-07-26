package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// opEntry is a transport adapter. Actor lifecycle and identity mutations all
// enter Controller through actorSystem; this type owns no actor map, gate, or
// execution state. Business policy (owner protection, declaration visibility,
// placement host) is resolved HERE, before the typed command is issued.
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

func (e *opEntry) Admit(ctx context.Context, req channel.AdmitRequest) (channel.AdmitResult, error) {
	if err := e.available(); err != nil {
		return channel.AdmitResult{}, err
	}
	if req.Principal == "" {
		return channel.AdmitResult{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "principal required",
		}
	}
	result, err := e.home.actors.Admit(ctx, actorctl.AdmitRequest{Principal: req.Principal})
	if err == nil && result.ActorID != "" {
		e.home.ensureSubjectSlot(result.ActorID)
		e.home.narrateBirth(ctx, result.ActorID, result.Created)
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
	return e.introduce(ctx, req.DeclID, req.InitiatorActorID, false)
}

func (e *opEntry) introduce(
	ctx context.Context,
	declID string,
	initiator actor.ActorID,
	memberSourced bool,
) (channel.IntroduceResult, error) {
	command, err := e.home.resolveIntroduction(ctx, declID, initiator, memberSourced)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	result, err := e.home.actors.Introduce(ctx, command)
	if err == nil && result.ActorID != "" {
		e.home.narrateBirth(ctx, result.ActorID, result.Created)
	}
	return result, err
}

func (e *opEntry) Remove(
	ctx context.Context,
	req channel.RemoveRequest,
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
	if err == nil {
		e.home.announceAudit(ctx, "remove_actor", map[string]any{
			"target": req.Target, "removed": result.Removed,
		})
	}
	return result, err
}

// AttachDaemon is a wiring-domain management action: it writes one binding row
// and touches no actor record.
func (e *opEntry) AttachDaemon(
	ctx context.Context,
	req channel.DaemonRequest,
) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	if req.DaemonID == "" {
		return channel.BindingResult{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "daemon_id required",
		}
	}
	created, err := e.home.cs.Bindings.AttachDaemon(
		ctx, storespec.DaemonID(req.DaemonID), e.home.nowMs())
	if err != nil {
		return channel.BindingResult{}, err
	}
	e.home.announceAudit(ctx, "attach_daemon", map[string]any{"daemon_id": req.DaemonID})
	if created {
		e.home.pokeReconcile()
	}
	homeActorEffects{home: e.home}.PlanPoke(executionDomain(req.DaemonID))
	return channel.BindingResult{Bound: true}, nil
}

// DetachDaemon removes the channel↔daemon binding row and NOTHING else. Actors
// placed on the detached daemon stay members: their desired dangles, execution
// fails closed, messages still arrive, and re-attaching the same daemon id
// reconciles them back. Cleaning up actors is a separate, explicit management
// action (ordinary End/Remove with an explicit target list).
func (e *opEntry) DetachDaemon(
	ctx context.Context,
	req channel.DaemonRequest,
) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	if req.DaemonID == "" {
		return channel.BindingResult{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "daemon_id required",
		}
	}
	if _, err := e.home.cs.Bindings.DetachDaemon(
		ctx, storespec.DaemonID(req.DaemonID)); err != nil {
		return channel.BindingResult{}, err
	}
	e.home.announceAudit(ctx, "detach_daemon", map[string]any{"daemon_id": req.DaemonID})
	if e.home.links != nil {
		e.home.links.KickDaemon(req.DaemonID)
	}
	e.home.pokeReconcile()
	homeActorEffects{home: e.home}.PlanPoke(executionDomain(req.DaemonID))
	return channel.BindingResult{Bound: false}, nil
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
			DeclID string `json:"decl_id"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.DeclID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: "decl_id required",
			}
		}
		result, err := e.introduce(ctx, payload.DeclID, req.Sender, true)
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
		result, err := e.Remove(ctx, channel.RemoveRequest{
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
		if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
			return nil, &sysactor.OperateError{
				Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required",
			}
		}
		if err := e.home.actors.Restart(ctx, actorctl.RestartRequest{
			ActorID: payload.InstanceID,
		}); err != nil {
			return nil, asOperateError(err)
		}
		e.home.announceAudit(ctx, "restart_actor", map[string]any{"target": payload.InstanceID})
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
		// A setting: last write wins, no dedup ceremony.
		if err := e.home.cs.Routing.SetDefaultAgent(ctx, payload.InstanceID); err != nil {
			if errors.Is(err, storespec.ErrMemberInactive) {
				return nil, &sysactor.OperateError{
					Code: string(channel.ErrCodeMemberInactive), Detail: "target is not an active member",
				}
			}
			return nil, asOperateError(err)
		}
		e.home.announceAudit(ctx, "set_default_agent", map[string]any{
			"default_agent": payload.InstanceID,
		})
		return map[string]any{"default_agent": payload.InstanceID}, nil

	default:
		return nil, &sysactor.OperateError{
			Code: string(channel.ErrCodeNotAcceptedSource), Detail: "operation is not accepted",
		}
	}
}

func executionDomain(daemonID string) actorhost.ExecutionDomain {
	return actorhost.ExecutionDomain(daemonID)
}

// narrateBirth writes the "joined the channel" narration for a freshly created
// record. A replayed birth (created=false) narrates nothing.
func (h *Home) narrateBirth(ctx context.Context, id actor.ActorID, created bool) {
	if !created {
		return
	}
	record, active, err := h.actors.LookupActive(ctx, id)
	if err != nil || !active {
		return
	}
	h.announceRegistered(ctx, record)
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
