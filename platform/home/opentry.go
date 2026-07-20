package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// opEntry is the sole serving-time structural operation component. It is
// intentionally unexported: sysactor owns the execution concept while Home
// supplies the physical channel-store and runtime convergence machinery.
type opEntry struct {
	home      *Home
	resolver  IntroductionResolver
	admission storespec.SysOpAdmission
	mu        sync.Mutex
}

const introductionResolveTimeout = 2 * time.Second

var (
	_ sysactor.OperateExecutor = (*opEntry)(nil)
	_ sysactor.SystemOps       = (*opEntry)(nil)
)

func (e *opEntry) available() error {
	if e == nil || e.home == nil || e.home.closed.Load() {
		return &channel.OperationError{Code: channel.ErrCodeChannelUnavailable, Detail: "channel is not serving", Retryable: true}
	}
	return nil
}

func systemMeta(ref string, request any) (storespec.SysOpMeta, error) {
	if strings.TrimSpace(ref) == "" {
		return storespec.SysOpMeta{}, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: "ref required"}
	}
	digest, err := channel.Digest(request)
	if err != nil {
		return storespec.SysOpMeta{}, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
	}
	return storespec.SysOpMeta{Anchor: channel.RefCorrelation(ref), RequestDigest: digest, Source: storespec.SysOpSourceSystem, Sender: actor.SystemActorID}, nil
}

func memberMeta(anchor string, sender actor.ActorID, request any) (storespec.SysOpMeta, error) {
	digest, err := channel.Digest(request)
	if err != nil {
		return storespec.SysOpMeta{}, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
	}
	return storespec.SysOpMeta{Anchor: channel.MessageCorrelation(anchor), RequestDigest: digest, Source: storespec.SysOpSourceMember, Sender: sender}, nil
}

func (e *opEntry) Admit(ctx context.Context, req channel.AdmitRequest) (channel.AdmitResult, error) {
	if err := e.available(); err != nil {
		return channel.AdmitResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.AdmitResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result, err := e.admission.Admit(ctx, storespec.AdmitTx{SysOpMeta: meta, Principal: req.Principal})
	if err != nil {
		return channel.AdmitResult{}, err
	}
	if err := e.publishActor(ctx, result.ActorID); err != nil {
		return channel.AdmitResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.AdmitResult{ActorID: result.ActorID, Created: result.Created}, nil
}

func (e *opEntry) Introduce(ctx context.Context, req channel.IntroduceRequest) (channel.IntroduceResult, error) {
	if err := e.available(); err != nil {
		return channel.IntroduceResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	result, err := e.introduce(ctx, meta, req.DeclID, req.InitiatorPrincipal, req.Rendered)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	return channel.IntroduceResult{ActorID: result.ActorID, Created: result.Created}, nil
}

func (e *opEntry) introduce(ctx context.Context, meta storespec.SysOpMeta, declID, initiator string, supplied *channel.RenderedSnapshot) (storespec.IntroduceResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Completed-anchor lookup deliberately precedes the external resolver. A
	// resolver outage can never hide a terminal that already committed.
	if completed, found, err := e.admission.LookupCompleted(ctx, meta.Anchor, meta.RequestDigest); err != nil {
		return storespec.IntroduceResult{}, err
	} else if found {
		if completed.ErrorCode != "" {
			return storespec.IntroduceResult{}, &channel.OperationError{Code: completed.ErrorCode, Detail: completed.ErrorDetail}
		}
		var result storespec.IntroduceResult
		if err := json.Unmarshal(completed.Result, &result); err != nil {
			return storespec.IntroduceResult{}, err
		}
		_ = e.publishActor(ctx, result.ActorID)
		return result, nil
	}
	if meta.DecisiveError != nil {
		_, err := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorPrincipal: initiator})
		return storespec.IntroduceResult{}, err
	}
	if e.resolver == nil {
		return storespec.IntroduceResult{}, &channel.OperationError{Code: channel.ErrCodeAuthorityUnavailable, Detail: "introduction resolver unavailable", Retryable: true}
	}
	resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
	facts, err := e.resolver.ResolveDeclaration(resolveCtx, e.home.channelID, declID)
	cancel()
	if err != nil {
		code := channel.ErrCodeAuthorityUnavailable
		retryable := true
		if errors.Is(err, channel.ErrDeclarationNotFound) {
			code, retryable = channel.ErrCodeDeclNotFound, false
		}
		opErr := &channel.OperationError{Code: code, Detail: err.Error(), Retryable: retryable}
		if retryable {
			return storespec.IntroduceResult{}, opErr
		}
		meta.DecisiveError = opErr
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorPrincipal: initiator})
		return storespec.IntroduceResult{}, recordErr
	}
	rendered := facts.Rendered
	if supplied != nil && supplied.RenderSeq > rendered.RenderSeq {
		rendered = *supplied
	}
	kind, err := e.resolver.ClassKind(ctx, rendered.Class)
	if err != nil {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeUnknownClass, Detail: err.Error()}
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorPrincipal: initiator})
		return storespec.IntroduceResult{}, recordErr
	}
	result, err := e.admission.Introduce(ctx, storespec.IntroduceTx{
		SysOpMeta: meta, DeclID: declID, InitiatorPrincipal: initiator,
		OwnerPrincipal: facts.OwnerPrincipal, Visibility: facts.Visibility,
		Kind: kind, Rendered: rendered,
	})
	if err != nil {
		return storespec.IntroduceResult{}, err
	}
	if err := e.publishActor(ctx, result.ActorID); err != nil {
		return storespec.IntroduceResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return result, nil
}

func (e *opEntry) AttachDaemon(ctx context.Context, req channel.DaemonRequest) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.BindingResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result, err := e.admission.AttachDaemon(ctx, storespec.AttachTx{SysOpMeta: meta, DaemonID: storespec.DaemonID(req.DaemonID)})
	if err != nil {
		return channel.BindingResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.BindingResult{Bound: result.Bound, ClearedInstances: result.ClearedInstances}, nil
}

func (e *opEntry) DetachDaemon(ctx context.Context, req channel.DaemonRequest) (channel.BindingResult, error) {
	if err := e.available(); err != nil {
		return channel.BindingResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.BindingResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result, err := e.admission.DetachDaemon(ctx, storespec.DetachTx{SysOpMeta: meta, DaemonID: storespec.DaemonID(req.DaemonID)})
	if err != nil {
		return channel.BindingResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.BindingResult{Bound: result.Bound, ClearedInstances: result.ClearedInstances}, nil
}

func (e *opEntry) ApplyDeclVersion(ctx context.Context, req channel.ApplyDeclVersionRequest) (channel.ApplyDeclVersionResult, error) {
	if err := e.available(); err != nil {
		return channel.ApplyDeclVersionResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.ApplyDeclVersionResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if completed, found, lookupErr := e.admission.LookupCompleted(ctx, meta.Anchor, meta.RequestDigest); lookupErr != nil {
		return channel.ApplyDeclVersionResult{}, lookupErr
	} else if found {
		if completed.ErrorCode != "" {
			return channel.ApplyDeclVersionResult{}, &channel.OperationError{Code: completed.ErrorCode, Detail: completed.ErrorDetail}
		}
		var replay channel.ApplyDeclVersionResult
		if err := json.Unmarshal(completed.Result, &replay); err != nil {
			return channel.ApplyDeclVersionResult{}, err
		}
		return replay, nil
	}
	var facts channel.DeclarationFacts
	if req.Authority == channel.AuthorityDelegate {
		if e.resolver == nil {
			return channel.ApplyDeclVersionResult{}, &channel.OperationError{Code: channel.ErrCodeAuthorityUnavailable, Detail: "introduction resolver unavailable", Retryable: true}
		}
		resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
		facts, err = e.resolver.ResolveDeclaration(resolveCtx, e.home.channelID, req.DeclID)
		cancel()
		if err != nil {
			if errors.Is(err, channel.ErrDeclarationNotFound) {
				meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeDeclNotFound, Detail: err.Error()}
				_, recordErr := e.admission.ApplyDeclVersion(ctx, storespec.ApplyTx{SysOpMeta: meta, DeclID: req.DeclID})
				return channel.ApplyDeclVersionResult{}, recordErr
			}
			return channel.ApplyDeclVersionResult{}, &channel.OperationError{Code: channel.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true}
		}
	}
	result, err := e.admission.ApplyDeclVersion(ctx, storespec.ApplyTx{
		SysOpMeta: meta, DeclID: req.DeclID, Rendered: req.Rendered, Authority: req.Authority,
		InitiatorPrincipal: req.InitiatorPrincipal, OwnerPrincipal: facts.OwnerPrincipal, Visibility: facts.Visibility,
	})
	if err != nil {
		return channel.ApplyDeclVersionResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.ApplyDeclVersionResult{Status: result.Status, Version: result.Version}, nil
}

func (e *opEntry) RevokeDeclTargets(ctx context.Context, req channel.RevokeDeclRequest) (channel.RevokeResult, error) {
	if err := e.available(); err != nil {
		return channel.RevokeResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.RevokeResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result, err := e.admission.RevokeDeclTargets(ctx, storespec.RevokeDeclTx{SysOpMeta: meta, DeclID: req.DeclID})
	if err != nil {
		return channel.RevokeResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.RevokeResult{PerInstance: result.PerInstance}, nil
}

func (e *opEntry) RevokeDaemon(ctx context.Context, req channel.DaemonRequest) (channel.RevokeResult, error) {
	if err := e.available(); err != nil {
		return channel.RevokeResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.RevokeResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result, err := e.admission.RevokeDaemon(ctx, storespec.RevokeDaemonTx{SysOpMeta: meta, DaemonID: storespec.DaemonID(req.DaemonID)})
	if err != nil {
		return channel.RevokeResult{}, err
	}
	e.applyEffects(ctx, result.Effects)
	return channel.RevokeResult{PerInstance: result.PerInstance}, nil
}

func (e *opEntry) publishActor(ctx context.Context, id actor.ActorID) error {
	row, found, err := e.home.cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil || !found {
		if err == nil {
			err = errors.New("committed actor missing from declared view")
		}
		return err
	}
	if _, already, _ := e.home.controlIndex.LookupActive(ctx, id); !already {
		if e.home.liveness.AdmitIdentity(id) != transitionApplied {
			return fmt.Errorf("platform: publish sysop actor %s: liveness rejected", id)
		}
	}
	if !e.home.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		return fmt.Errorf("platform: publish sysop actor %s: control index rejected", id)
	}
	if row.Kind == actor.KindHuman {
		e.home.EnsureSubjectSlot(id)
	}
	e.home.pokeReconcile()
	return nil
}

func (e *opEntry) applyEffects(ctx context.Context, effects storespec.PostCommitEffects) {
	for _, id := range effects.Despawn {
		if row, active, err := e.home.cs.Declared.LookupDeclaredActive(ctx, id); err == nil && active {
			_ = e.home.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}})
			_, _ = e.home.liveness.Retire(id, true)
		} else if err == nil {
			e.home.controlIndex.DeleteBatch([]actor.ActorID{id})
			_, _ = e.home.liveness.EndIdentity(id)
			e.home.RemoveSubjectSlot(id)
			e.home.presenceFold.Forget(id)
		}
		e.home.channel.Cells().DespawnIDReason(id, "sysop")
	}
	if effects.KickDaemon != nil && e.home.links != nil {
		e.home.links.KickDaemon(string(*effects.KickDaemon))
	}
	for _, principal := range effects.Principals {
		if e.home.onMembershipChange != nil {
			e.home.onMembershipChange(principal)
		}
	}
	if effects.Poke {
		e.home.pokeReconcile()
	}
}

// Execute adapts the member mailbox transport while keeping reply ownership in
// sysactor. The operation table is closed here; adding another state noun does
// not grow the transport contract.
func (e *opEntry) Execute(ctx context.Context, operation string, req sysactor.OperateRequest) (any, error) {
	switch operation {
	case sysactor.TypeIntroduceActor:
		return e.executeMemberIntroduce(ctx, req)
	case sysactor.TypeRemoveActor:
		return e.executeMemberRemove(ctx, req)
	case sysactor.TypeRestartActor:
		return e.executeMemberRestart(ctx, req)
	case sysactor.TypeSetDefaultAgent:
		return e.executeMemberSetDefault(ctx, req)
	default:
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeNotAcceptedSource), Detail: "operation is not accepted"}
	}
}

func (e *opEntry) executeMemberIntroduce(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	var payload struct {
		DeclID string `json:"decl_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.DeclID == "" {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: "decl_id required"}
	}
	meta, err := memberMeta(req.Anchor, req.Sender, payload)
	if err != nil {
		return nil, err
	}
	row, found, err := e.home.controlIndex.LookupActive(ctx, req.Sender)
	if err != nil || !found {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeUnauthorizedSender, Detail: "sender is not active"}
		_, decisionErr := e.introduce(ctx, meta, payload.DeclID, "", nil)
		return nil, asOperateError(decisionErr)
	}
	result, err := e.introduce(ctx, meta, payload.DeclID, row.Principal, nil)
	if err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"instance_id": result.ActorID, "created": result.Created}, nil
}

func (e *opEntry) executeMemberRemove(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required"}
	}
	if err := e.home.systemEndHandle().End(ctx, payload.InstanceID, "member_remove"); err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"removed": payload.InstanceID}, nil
}

func (e *opEntry) executeMemberRestart(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required"}
	}
	if _, err := e.home.RestartInstanceDirect(ctx, payload.InstanceID); err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"restarted": payload.InstanceID}, nil
}

func (e *opEntry) executeMemberSetDefault(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: err.Error()}
	}
	if err := e.home.SetDefaultAgent(ctx, payload.InstanceID); err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"default_agent": payload.InstanceID}, nil
}

func asOperateError(err error) error {
	var operationErr *channel.OperationError
	if errors.As(err, &operationErr) {
		return &sysactor.OperateError{Code: string(operationErr.Code), Detail: operationErr.Detail}
	}
	return err
}

// SystemOps is an assembly-only bridge used by ChannelHost. It is a package
// function rather than a Home method so the Home capability surface does not
// grow an execution escape hatch.
func SystemOps(h *Home) sysactor.SystemOps {
	if h == nil {
		return nil
	}
	return h.opEntry
}
