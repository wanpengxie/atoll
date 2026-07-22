package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	sync      storespec.DeclarationSyncStore
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
	e.applyEffects(result.Effects)
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
	result, err := e.introduce(ctx, meta, req.DeclID, req.InitiatorActorID)
	if err != nil {
		return channel.IntroduceResult{}, err
	}
	return channel.IntroduceResult{ActorID: result.ActorID, Created: result.Created}, nil
}

func (e *opEntry) introduce(ctx context.Context, meta storespec.SysOpMeta, declID string, initiator actor.ActorID) (storespec.IntroduceResult, error) {
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
		if err := e.publishActor(ctx, result.ActorID); err != nil {
			return storespec.IntroduceResult{}, err
		}
		return result, nil
	}
	if meta.DecisiveError != nil {
		_, err := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator})
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
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator})
		return storespec.IntroduceResult{}, recordErr
	}
	rendered, err := (channel.RenderedSnapshot{
		Class: facts.Class, Config: append(json.RawMessage(nil), facts.Config...),
		Placement: channel.Placement{Kind: channel.PlacementDaemon},
	}).Seal()
	if err != nil {
		return storespec.IntroduceResult{}, &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: err.Error()}
	}
	// ClassKind is a resolver call like ResolveDeclaration: fail-closed on its
	// own bounded window, and only the definitive "no such class" answer
	// (found=false) is decisive — ANY resolver error (timeout, I/O, registry
	// fault) is retryable authority_unavailable, never a permanent
	// unknown_class terminal.
	kindCtx, cancelKind := context.WithTimeout(ctx, introductionResolveTimeout)
	kind, found, err := e.resolver.ClassKind(kindCtx, rendered.Class)
	cancelKind()
	if err != nil {
		return storespec.IntroduceResult{}, &channel.OperationError{Code: channel.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true}
	}
	if !found {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeUnknownClass, Detail: "unknown class " + rendered.Class}
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator})
		return storespec.IntroduceResult{}, recordErr
	}
	if initiator == "" {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: "initiator_actor_id required"}
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID})
		return storespec.IntroduceResult{}, recordErr
	}
	releaseInitiator := e.home.actorGates.lock(initiator)
	defer releaseInitiator()
	initiatorRow, active, lookupErr := e.home.controlIndex.LookupActive(ctx, initiator)
	if lookupErr != nil {
		return storespec.IntroduceResult{}, lookupErr
	}
	if !active {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeMemberInactive, Detail: "initiator is not an active member"}
		_, recordErr := e.admission.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator})
		return storespec.IntroduceResult{}, recordErr
	}
	result, err := e.admission.Introduce(ctx, storespec.IntroduceTx{
		SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator, InitiatorPrincipal: initiatorRow.Principal,
		OwnerPrincipal: facts.OwnerPrincipal, Visibility: facts.Visibility,
		Kind: kind, Rendered: rendered,
	})
	if err != nil {
		return storespec.IntroduceResult{}, err
	}
	if err := e.publishActor(ctx, result.ActorID); err != nil {
		return storespec.IntroduceResult{}, err
	}
	e.applyEffects(result.Effects)
	return result, nil
}

func (e *opEntry) Remove(ctx context.Context, req channel.RemoveRequest) (channel.RemoveResult, error) {
	if err := e.available(); err != nil {
		return channel.RemoveResult{}, err
	}
	meta, err := systemMeta(req.Ref, req)
	if err != nil {
		return channel.RemoveResult{}, err
	}
	result, err := e.remove(ctx, meta, req.Target, req.InitiatorActorID, "system_remove")
	if err != nil {
		return channel.RemoveResult{}, err
	}
	return channel.RemoveResult{Removed: result.Removed}, nil
}

func (e *opEntry) remove(ctx context.Context, meta storespec.SysOpMeta, target, initiator actor.ActorID, reason string) (storespec.RemoveResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if completed, found, err := e.admission.LookupCompleted(ctx, meta.Anchor, meta.RequestDigest); err != nil {
		return storespec.RemoveResult{}, err
	} else if found {
		if completed.ErrorCode != "" {
			return storespec.RemoveResult{}, &channel.OperationError{Code: completed.ErrorCode, Detail: completed.ErrorDetail}
		}
		var result storespec.RemoveResult
		if err := json.Unmarshal(completed.Result, &result); err != nil {
			return storespec.RemoveResult{}, err
		}
		return result, nil
	}
	if target == "" || initiator == "" {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: "target and initiator_actor_id required"}
		return e.admission.RemoveActor(ctx, storespec.RemoveTx{SysOpMeta: meta, Target: target, InitiatorActorID: initiator, Reason: reason})
	}
	h := e.home
	if h.closed.Load() {
		return storespec.RemoveResult{}, ErrClosed
	}
	locked := map[actor.ActorID]bool{}
	releases := []func(){}
	lockOne := func(id actor.ActorID) {
		if id == "" || locked[id] {
			return
		}
		releases = append(releases, h.actorGates.lock(id))
		locked[id] = true
	}
	lockOne(initiator)
	lockOne(target)
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	if _, active, err := h.controlIndex.LookupActive(ctx, initiator); err != nil {
		return storespec.RemoveResult{}, err
	} else if !active {
		meta.DecisiveError = &channel.OperationError{Code: channel.ErrCodeMemberInactive, Detail: "initiator is not an active member"}
		return e.admission.RemoveActor(ctx, storespec.RemoveTx{SysOpMeta: meta, Target: target, InitiatorActorID: initiator, Reason: reason})
	}
	if err := h.lockClosure(ctx, target, locked, lockOne); err != nil {
		return storespec.RemoveResult{}, err
	}
	plan, err := h.buildEndPlan(ctx, target, reason, actor.SystemActorID)
	if err != nil {
		return storespec.RemoveResult{}, err
	}
	result, err := e.admission.RemoveActor(ctx, storespec.RemoveTx{
		SysOpMeta: meta, Target: target, InitiatorActorID: initiator, Reason: reason,
		DurableIDs: plan.DurableIDs, Envelopes: plan.Envelopes,
	})
	if err != nil {
		return storespec.RemoveResult{}, err
	}
	h.finishEndTeardown(plan)()
	h.pokeReconcile()
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
	e.applyEffects(result.Effects)
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
	return e.detachDaemon(ctx, meta, req.DaemonID)
}

func (e *opEntry) detachDaemon(ctx context.Context, meta storespec.SysOpMeta, daemonID string) (channel.BindingResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h := e.home
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return channel.BindingResult{}, err
	}
	var roots []actor.ActorID
	for _, row := range rows {
		if row.Role == storespec.RoleOwner || row.ID == actor.SystemActorID || row.Placement.Kind != storespec.PlacementDaemon || row.Placement.Host != daemonID {
			continue
		}
		roots = append(roots, row.ID)
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i] < roots[j] })
	locked := map[actor.ActorID]bool{}
	releases := []func(){}
	lockOne := func(id actor.ActorID) {
		if id == "" || locked[id] {
			return
		}
		releases = append(releases, h.actorGates.lock(id))
		locked[id] = true
	}
	for _, root := range roots {
		lockOne(root)
	}
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, root := range roots {
		if err := h.lockClosure(ctx, root, locked, lockOne); err != nil {
			return channel.BindingResult{}, err
		}
	}
	plan, err := h.buildDaemonDetachPlan(ctx, roots, daemonID)
	if err != nil {
		return channel.BindingResult{}, err
	}
	result, err := e.admission.DetachDaemon(ctx, storespec.DetachTx{
		SysOpMeta: meta, DaemonID: storespec.DaemonID(daemonID), DurableIDs: plan.DurableIDs,
		AllIDs: plan.AllIDs, Envelopes: plan.Envelopes,
	})
	if err != nil {
		return channel.BindingResult{}, err
	}
	var tail func()
	if len(plan.AllIDs) > 0 {
		tail = h.finishEndTeardown(plan)
	}
	e.applyEffects(result.Effects)
	if tail != nil {
		tail()
	}
	return channel.BindingResult{Bound: result.Bound, ClearedInstances: result.ClearedInstances}, nil
}

// applyResolvedDeclaration is the private, ActorID-exact write half of the
// declaration pull arm. ResolveDeclaration is always called by the caller
// before entering this serial section; inside it we re-read identity and every
// channel-owned field before the store performs its same-transaction hash
// comparison and version advance.
func (e *opEntry) applyResolvedDeclaration(ctx context.Context, id actor.ActorID, declID, class string, config json.RawMessage) (storespec.DeclarationSyncResult, error) {
	if err := e.available(); err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	release := e.home.actorGates.lock(id)
	defer release()
	current, active, err := e.home.controlIndex.LookupActive(ctx, id)
	if err != nil || !active || current.SourceDeclID != declID {
		return storespec.DeclarationSyncResult{}, err
	}
	if current.Kind != actor.KindAgent && current.Kind != actor.KindTool {
		return storespec.DeclarationSyncResult{}, nil
	}
	kindCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
	kind, found, err := e.resolver.ClassKind(kindCtx, class)
	cancel()
	if err != nil || !found || kind != current.Kind {
		return storespec.DeclarationSyncResult{}, err
	}
	request := struct {
		ActorID actor.ActorID   `json:"actor_id"`
		DeclID  string          `json:"decl_id"`
		Class   string          `json:"class"`
		Config  json.RawMessage `json:"config,omitempty"`
	}{id, declID, class, config}
	meta, err := systemMeta("ifin:v1:"+uuid.NewString(), request)
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	result, err := e.sync.ApplyResolvedDeclaration(ctx, storespec.DeclarationSyncTx{
		SysOpMeta: meta, ActorID: id, DeclID: declID, Class: class,
		Config: append(json.RawMessage(nil), config...),
	})
	if err != nil {
		return storespec.DeclarationSyncResult{}, err
	}
	if result.Status == storespec.DeclarationApplied || result.Status == storespec.DeclarationEqual {
		// The pull arm already holds the exact actor gate and knows the value the
		// store compared. Publish it here. Publishing Equal as well heals the
		// narrow commit-before-publication crash window on the next ordinary pull,
		// without a second full-registry repair mechanism.
		current.Class = class
		current.Config = append(json.RawMessage(nil), config...)
		current.CurrentDeclVersion = result.Version
		if !e.home.controlIndex.UpsertBatch([]controlEntry{{Row: current, World: storespec.WorldDurable}}) {
			return storespec.DeclarationSyncResult{}, fmt.Errorf("platform: publish declaration sync actor %s", id)
		}
	}
	if result.Status == storespec.DeclarationApplied {
		e.applyEffects(result.Effects)
	}
	return result, nil
}

func (e *opEntry) publishActor(_ context.Context, id actor.ActorID) error {
	// The identity transaction has already committed. Publication is substrate
	// bookkeeping and must not be skipped merely because the request deadline
	// expired in the commit-to-return window.
	publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.home.publishDeclaredActor(publishCtx, id, storespec.RoleNone)
	return err
}

func (e *opEntry) applyEffects(effects storespec.PostCommitEffects) {
	for _, id := range effects.Despawn {
		// Identity mutations publish their cache value at the owning call site;
		// this shared effect is incarnation-only.
		_, _ = e.home.liveness.Retire(id, true)
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
	result, err := e.introduce(ctx, meta, payload.DeclID, req.Sender)
	if err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"instance_id": result.ActorID, "created": result.Created}, nil
}

// executeMemberRemove folds the remove word into the value paradigm: the
// closure is computed under Home's Fork-race lock discipline (the sole holder of
// the run-world sponsor graph), then RemoveActor commits the cascade value rows
// with the anchor + started/completed event pair in ONE transaction. No
// execution path is touched; despawn is a reconcile private
// matter carried out here as the post-commit teardown.
func (e *opEntry) executeMemberRemove(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required"}
	}
	meta, err := memberMeta(req.Anchor, req.Sender, payload)
	if err != nil {
		return nil, err
	}
	res, err := e.remove(ctx, meta, payload.InstanceID, req.Sender, "member_remove")
	if err != nil {
		return nil, asOperateError(err)
	}
	return map[string]any{"removed": res.Removed}, nil
}

func (e *opEntry) executeMemberRestart(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.InstanceID == "" {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: "instance_id required"}
	}
	meta, err := memberMeta(req.Anchor, req.Sender, payload)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	res, err := e.admission.RestartActor(ctx, storespec.RestartTx{SysOpMeta: meta, Target: payload.InstanceID})
	if err != nil {
		return nil, asOperateError(err)
	}
	// Restart is an incarnation-axis effect: applyEffects retires the current
	// body with restart intent in the liveness ledger and reconcile mints the
	// next one. Identity truth and the control index are untouched — restart
	// has no identity-axis value to publish.
	e.applyEffects(res.Effects)
	return map[string]any{"restarted": payload.InstanceID}, nil
}

func (e *opEntry) executeMemberSetDefault(ctx context.Context, req sysactor.OperateRequest) (any, error) {
	if err := e.available(); err != nil {
		return nil, asOperateError(err)
	}
	var payload struct {
		InstanceID actor.ActorID `json:"instance_id"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &sysactor.OperateError{Code: string(channel.ErrCodeBadPayload), Detail: err.Error()}
	}
	meta, err := memberMeta(req.Anchor, req.Sender, payload)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	res, err := e.admission.SetDefaultAgent(ctx, storespec.SetDefaultTx{SysOpMeta: meta, Target: payload.InstanceID})
	if err != nil {
		return nil, asOperateError(err)
	}
	e.applyEffects(res.Effects)
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
