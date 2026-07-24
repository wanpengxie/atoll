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

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// homeActorStore adapts the existing channel value stores to actorctl's typed
// command port. Durable actor rows remain in the channel store. Run-world
// children live only for this Home session; their operation result is durable
// solely to make a Fork RequestID idempotent.
type homeActorStore struct {
	channelID channel.ID
	cs        *runtime.ChannelStores
	resolver  IntroductionResolver
	now       func() time.Time

	authorityMu sync.RWMutex
	authority   storespec.ActorAuthority

	runMu   sync.RWMutex
	runRows map[actor.ActorID]storespec.ActorControlRow
}

func newHomeActorStore(
	channelID channel.ID,
	cs *runtime.ChannelStores,
	resolver IntroductionResolver,
	now func() time.Time,
) *homeActorStore {
	if now == nil {
		now = time.Now
	}
	return &homeActorStore{
		channelID: channelID,
		cs:        cs,
		resolver:  resolver,
		now:       now,
		runRows:   make(map[actor.ActorID]storespec.ActorControlRow),
	}
}

func (s *homeActorStore) bindAuthority(authority storespec.ActorAuthority) {
	s.authorityMu.Lock()
	s.authority = authority
	s.authorityMu.Unlock()
}

func (s *homeActorStore) activeAuthority() storespec.ActorAuthority {
	s.authorityMu.RLock()
	defer s.authorityMu.RUnlock()
	return s.authority
}

func (s *homeActorStore) ListDeclaredActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	return s.cs.Declared.ListDeclaredActive(ctx)
}

func cloneActorRow(row storespec.ActorControlRow) storespec.ActorControlRow {
	row.Config = append(json.RawMessage(nil), row.Config...)
	return row
}

func (s *homeActorStore) LookupActive(
	ctx context.Context,
	id actor.ActorID,
) (actorctl.StoredActor, bool, error) {
	s.runMu.RLock()
	run, ok := s.runRows[id]
	s.runMu.RUnlock()
	if ok {
		return actorctl.StoredActor{Row: cloneActorRow(run), Origin: actorctl.OriginRunWorld}, true, nil
	}
	row, ok, err := s.cs.Declared.LookupDeclaredActive(ctx, id)
	return actorctl.StoredActor{Row: cloneActorRow(row), Origin: actorctl.OriginDurable}, ok, err
}

func (s *homeActorStore) Admit(
	ctx context.Context,
	request actorctl.AdmitRequest,
) (actorctl.ActorCommit[actorctl.AdmitResult], error) {
	if request.Role == storespec.RoleOwner {
		result, err := s.cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
			Kind: actor.KindHuman, Principal: request.Principal, Class: "human",
			Role: request.Role, Placement: storespec.NewServerPlacement(),
			CreatedAt: s.now().UnixMilli(),
		})
		if err != nil {
			return actorctl.ActorCommit[actorctl.AdmitResult]{}, err
		}
		stored, ok, err := s.LookupActive(ctx, result.ID)
		if err != nil {
			return actorctl.ActorCommit[actorctl.AdmitResult]{}, err
		}
		if !ok {
			return actorctl.ActorCommit[actorctl.AdmitResult]{}, storespec.ErrActorNotFound
		}
		return actorctl.ActorCommit[actorctl.AdmitResult]{
			Actor: stored,
			Result: channel.AdmitResult{
				ActorID: result.ID,
				Created: result.Created,
			},
			Effects: storespec.PostCommitEffects{
				Poke:       true,
				Principals: []string{request.Principal},
			},
		}, nil
	}
	meta, err := systemMeta(request.Ref, request)
	if err != nil {
		return actorctl.ActorCommit[actorctl.AdmitResult]{}, err
	}
	result, err := s.cs.SysOps.Admit(ctx, storespec.AdmitTx{
		SysOpMeta: meta,
		Principal: request.Principal,
	})
	if err != nil {
		return actorctl.ActorCommit[actorctl.AdmitResult]{}, err
	}
	stored, ok, err := s.LookupActive(ctx, result.ActorID)
	if err != nil {
		return actorctl.ActorCommit[actorctl.AdmitResult]{}, err
	}
	if !ok {
		return actorctl.ActorCommit[actorctl.AdmitResult]{}, storespec.ErrActorNotFound
	}
	return actorctl.ActorCommit[actorctl.AdmitResult]{
		Actor: stored,
		Result: channel.AdmitResult{
			ActorID: result.ActorID,
			Created: result.Created,
		},
		Effects: result.Effects,
	}, nil
}

func (s *homeActorStore) Introduce(
	ctx context.Context,
	request actorctl.IntroduceRequest,
) (actorctl.ActorCommit[actorctl.IntroduceResult], error) {
	meta, err := actorCommandMeta(
		request.Ref,
		request.Member,
		struct {
			DeclID           string        `json:"decl_id"`
			InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
		}{request.DeclID, request.InitiatorActorID},
	)
	if err != nil {
		return actorctl.ActorCommit[actorctl.IntroduceResult]{}, err
	}
	result, err := s.introduce(ctx, meta, request.DeclID, request.InitiatorActorID)
	if err != nil {
		return actorctl.ActorCommit[actorctl.IntroduceResult]{}, err
	}
	stored, ok, err := s.LookupActive(ctx, result.ActorID)
	if err != nil {
		return actorctl.ActorCommit[actorctl.IntroduceResult]{}, err
	}
	if !ok {
		return actorctl.ActorCommit[actorctl.IntroduceResult]{}, storespec.ErrActorNotFound
	}
	return actorctl.ActorCommit[actorctl.IntroduceResult]{
		Actor: stored,
		Result: channel.IntroduceResult{
			ActorID: result.ActorID,
			Created: result.Created,
		},
		Effects: result.Effects,
	}, nil
}

func (s *homeActorStore) introduce(
	ctx context.Context,
	meta storespec.SysOpMeta,
	declID string,
	initiator actor.ActorID,
) (storespec.IntroduceResult, error) {
	if completed, found, err := s.cs.SysOps.LookupCompleted(ctx, meta.Anchor, meta.RequestDigest); err != nil {
		return storespec.IntroduceResult{}, err
	} else if found {
		if completed.ErrorCode != "" {
			return storespec.IntroduceResult{}, &channel.OperationError{
				Code: completed.ErrorCode, Detail: completed.ErrorDetail,
			}
		}
		var result storespec.IntroduceResult
		if err := json.Unmarshal(completed.Result, &result); err != nil {
			return storespec.IntroduceResult{}, err
		}
		return result, nil
	}
	if s.resolver == nil {
		return storespec.IntroduceResult{}, &channel.OperationError{
			Code: channel.ErrCodeAuthorityUnavailable, Detail: "introduction resolver unavailable", Retryable: true,
		}
	}
	resolveCtx, cancel := context.WithTimeout(ctx, introductionResolveTimeout)
	facts, err := s.resolver.ResolveDeclaration(resolveCtx, s.channelID, declID)
	cancel()
	if err != nil {
		code, retryable := channel.ErrCodeAuthorityUnavailable, true
		if errors.Is(err, channel.ErrDeclarationNotFound) {
			code, retryable = channel.ErrCodeDeclNotFound, false
		}
		opErr := &channel.OperationError{Code: code, Detail: err.Error(), Retryable: retryable}
		if retryable {
			return storespec.IntroduceResult{}, opErr
		}
		meta.DecisiveError = opErr
		return s.cs.SysOps.Introduce(ctx, storespec.IntroduceTx{
			SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator,
		})
	}
	rendered, err := (channel.RenderedSnapshot{
		Class:     facts.Class,
		Config:    append(json.RawMessage(nil), facts.Config...),
		Placement: channel.Placement{Kind: channel.PlacementDaemon},
	}).Seal()
	if err != nil {
		return storespec.IntroduceResult{}, &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: err.Error(),
		}
	}
	kindCtx, cancelKind := context.WithTimeout(ctx, introductionResolveTimeout)
	kind, found, err := s.resolver.ClassKind(kindCtx, rendered.Class)
	cancelKind()
	if err != nil {
		return storespec.IntroduceResult{}, &channel.OperationError{
			Code: channel.ErrCodeAuthorityUnavailable, Detail: err.Error(), Retryable: true,
		}
	}
	if !found {
		meta.DecisiveError = &channel.OperationError{
			Code: channel.ErrCodeUnknownClass, Detail: "unknown class " + rendered.Class,
		}
		return s.cs.SysOps.Introduce(ctx, storespec.IntroduceTx{
			SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator,
		})
	}
	if initiator == "" {
		meta.DecisiveError = &channel.OperationError{
			Code: channel.ErrCodeBadPayload, Detail: "initiator_actor_id required",
		}
		return s.cs.SysOps.Introduce(ctx, storespec.IntroduceTx{SysOpMeta: meta, DeclID: declID})
	}
	authority := s.activeAuthority()
	if authority == nil {
		return storespec.IntroduceResult{}, actorctl.ErrBootstrapping
	}
	initiatorRow, active, err := authority.LookupActive(ctx, initiator)
	if err != nil {
		return storespec.IntroduceResult{}, err
	}
	if !active {
		meta.DecisiveError = &channel.OperationError{
			Code: channel.ErrCodeMemberInactive, Detail: "initiator is not an active member",
		}
		return s.cs.SysOps.Introduce(ctx, storespec.IntroduceTx{
			SysOpMeta: meta, DeclID: declID, InitiatorActorID: initiator,
		})
	}
	return s.cs.SysOps.Introduce(ctx, storespec.IntroduceTx{
		SysOpMeta:          meta,
		DeclID:             declID,
		InitiatorActorID:   initiator,
		InitiatorPrincipal: initiatorRow.Principal,
		OwnerPrincipal:     facts.OwnerPrincipal,
		Visibility:         facts.Visibility,
		Kind:               kind,
		Rendered:           rendered,
	})
}

func forkAnchor(caller actor.ActorID, request message.ID) string {
	return channel.MessageCorrelation(string(caller) + "\x00" + string(request))
}

func (s *homeActorStore) LookupFork(
	ctx context.Context,
	caller actor.ActorID,
	request message.ID,
) (actor.ActorID, bool, error) {
	completed, found, err := s.cs.SysOps.LookupCompleted(ctx, forkAnchor(caller, request), "")
	if err != nil || !found {
		return "", found, err
	}
	if completed.ErrorCode != "" {
		return "", true, &channel.OperationError{
			Code: completed.ErrorCode, Detail: completed.ErrorDetail,
		}
	}
	var result storespec.ForkResult
	if err := json.Unmarshal(completed.Result, &result); err != nil {
		return "", true, err
	}
	// The durable operation anchor is only a RequestID→ChildActorID receipt.
	// A run-world child belongs to the process that created it; replaying the
	// receipt after restart must not restore that child's definition into the
	// current in-memory run world.
	return result.Child.ID, true, nil
}

func (s *homeActorStore) CommitFork(
	ctx context.Context,
	request actorctl.ForkCommitRequest,
) (actorctl.ForkCommitResult, error) {
	operation := struct {
		Caller actor.ActorID    `json:"caller"`
		Spec   actorcapsForkDTO `json:"spec"`
	}{
		Caller: request.CallerActorID,
		Spec: actorcapsForkDTO{
			Kind: request.Spec.Kind, Class: request.Spec.Class,
			NameHint: request.Spec.NameHint, Config: request.Spec.Config,
			Placement: request.Spec.Placement,
		},
	}
	digest, err := channel.Digest(operation)
	if err != nil {
		return actorctl.ForkCommitResult{}, err
	}
	meta := storespec.SysOpMeta{
		Anchor:        forkAnchor(request.CallerActorID, request.RequestID),
		RequestDigest: digest,
		Source:        storespec.SysOpSourceMember,
		Sender:        request.CallerActorID,
	}
	row := storespec.ActorControlRow{
		ID:                 request.ChildActorID,
		Kind:               request.Spec.Kind,
		CreatedAt:          s.now().UnixMilli(),
		CurrentDeclVersion: 1,
		Class:              request.Spec.Class,
		Config:             append(json.RawMessage(nil), request.Spec.Config...),
		Placement:          request.Placement,
	}
	if request.Placement.Kind == storespec.PlacementDaemon {
		row.Binding = actor.BindingRuntimeInboundViaRelay
	}
	result, err := s.cs.SysOps.ForkActor(ctx, storespec.ForkTx{SysOpMeta: meta, Child: row})
	if err != nil {
		return actorctl.ForkCommitResult{}, err
	}
	row = cloneActorRow(result.Child)
	s.runMu.Lock()
	s.runRows[row.ID] = row
	s.runMu.Unlock()
	return actorctl.ForkCommitResult{
		ChildActorID: row.ID,
		Actor: actorctl.StoredActor{
			Row: row, Origin: actorctl.OriginRunWorld,
		},
		Effects: storespec.PostCommitEffects{Poke: true},
	}, nil
}

// actorcapsForkDTO intentionally mirrors only the actor-facing Fork operation
// value. ChildActorID and AttemptKey are physical choices and cannot affect the
// operation digest.
type actorcapsForkDTO struct {
	Kind      actor.Kind         `json:"kind"`
	Class     string             `json:"class"`
	NameHint  string             `json:"name_hint,omitempty"`
	Config    json.RawMessage    `json:"config,omitempty"`
	Placement *channel.Placement `json:"placement,omitempty"`
}

func (s *homeActorStore) Restart(
	ctx context.Context,
	request actorctl.RestartRequest,
) (actorctl.ActorCommit[struct{}], error) {
	stored, active, err := s.LookupActive(ctx, request.ActorID)
	if err != nil || !active {
		if err == nil {
			err = actorctl.ErrInactive
		}
		return actorctl.ActorCommit[struct{}]{}, err
	}
	meta, err := memberMeta(string(request.RequestID), request.CallerActorID, struct {
		ActorID actor.ActorID `json:"actor_id"`
	}{request.ActorID})
	if err != nil {
		return actorctl.ActorCommit[struct{}]{}, err
	}
	result, err := s.cs.SysOps.RestartActor(ctx, storespec.RestartTx{
		SysOpMeta: meta,
		Target:    request.ActorID,
	})
	if err != nil {
		return actorctl.ActorCommit[struct{}]{}, err
	}
	return actorctl.ActorCommit[struct{}]{
		Actor: stored, Effects: result.Effects,
	}, nil
}

func (s *homeActorStore) ApplyDeclaration(
	ctx context.Context,
	change actorctl.DeclarationChange,
) (actorctl.ActorCommit[struct{}], error) {
	request := struct {
		ActorID actor.ActorID   `json:"actor_id"`
		Class   string          `json:"class"`
		Config  json.RawMessage `json:"config,omitempty"`
	}{change.ActorID, change.Class, change.Config}
	ref := string(change.RequestID)
	if strings.TrimSpace(ref) == "" {
		ref = "ifin:v1:" + fmt.Sprint(s.now().UnixNano())
	}
	meta, err := systemMeta(ref, request)
	if err != nil {
		return actorctl.ActorCommit[struct{}]{}, err
	}
	current, active, err := s.LookupActive(ctx, change.ActorID)
	if err != nil || !active {
		if err == nil {
			err = actorctl.ErrInactive
		}
		return actorctl.ActorCommit[struct{}]{}, err
	}
	result, err := s.cs.DeclarationSync.ApplyResolvedDeclaration(ctx, storespec.DeclarationSyncTx{
		SysOpMeta: meta,
		ActorID:   change.ActorID,
		DeclID:    current.Row.SourceDeclID,
		Class:     change.Class,
		Config:    append(json.RawMessage(nil), change.Config...),
	})
	if err != nil {
		return actorctl.ActorCommit[struct{}]{}, err
	}
	stored, active, err := s.LookupActive(ctx, change.ActorID)
	if err != nil || !active {
		if err == nil {
			err = actorctl.ErrInactive
		}
		return actorctl.ActorCommit[struct{}]{}, err
	}
	return actorctl.ActorCommit[struct{}]{
		Actor: stored, Effects: result.Effects,
	}, nil
}

func (s *homeActorStore) AttachDaemon(
	ctx context.Context,
	request actorctl.AttachDaemonRequest,
) (actorctl.ValueCommit[actorctl.AttachDaemonResult], error) {
	meta, err := systemMeta(request.Ref, request)
	if err != nil {
		return actorctl.ValueCommit[actorctl.AttachDaemonResult]{}, err
	}
	result, err := s.cs.SysOps.AttachDaemon(ctx, storespec.AttachTx{
		SysOpMeta: meta,
		DaemonID:  storespec.DaemonID(request.DaemonID),
	})
	if err != nil {
		return actorctl.ValueCommit[actorctl.AttachDaemonResult]{}, err
	}
	return actorctl.ValueCommit[actorctl.AttachDaemonResult]{
		Result: channel.BindingResult{
			Bound: result.Bound, ClearedInstances: result.ClearedInstances,
		},
		Effects: result.Effects,
	}, nil
}

type terminalStorePlan struct {
	all        []actor.ActorID
	durable    []actor.ActorID
	run        []actor.ActorID
	envelopes  []storespec.CascadeEnvelope
	principals []string
}

func (s *homeActorStore) ResolveTerminal(
	_ context.Context,
	command actorctl.TerminalCommand,
	rows []storespec.ActorControlRow,
) (actorctl.TerminalPlan, error) {
	byID := make(map[actor.ActorID]storespec.ActorControlRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	var roots []actor.ActorID
	var reason string
	var endedBy actor.ActorID
	switch command.Kind {
	case actorctl.TerminalEnd:
		target := command.End.Target
		if target == actor.SystemActorID {
			return actorctl.TerminalPlan{}, ErrEndSystemForbidden
		}
		row, active := byID[target]
		if !active {
			return actorctl.TerminalPlan{}, nil
		}
		if row.Role == storespec.RoleOwner {
			return actorctl.TerminalPlan{}, storespec.ErrChannelOwnerProtected
		}
		caller := command.End.CallerActorID
		if caller != actor.SystemActorID && caller != target && row.Sponsor != caller {
			return actorctl.TerminalPlan{}, ErrEndNotSponsor
		}
		roots = []actor.ActorID{target}
		reason, endedBy = command.End.Reason, caller
	case actorctl.TerminalRemove:
		request := command.Remove
		if request.Target == actor.SystemActorID {
			return actorctl.TerminalPlan{}, ErrEndSystemForbidden
		}
		if request.InitiatorActorID == "" || byID[request.InitiatorActorID].ID == "" {
			return actorctl.TerminalPlan{}, ErrEndNotMember
		}
		if _, active := byID[request.Target]; active {
			roots = []actor.ActorID{request.Target}
		}
		reason, endedBy = "system_remove", actor.SystemActorID
	case actorctl.TerminalDetachDaemon:
		daemon := command.Detach.DaemonID
		for _, row := range rows {
			if row.ID != actor.SystemActorID && row.Role != storespec.RoleOwner &&
				row.Placement.Kind == storespec.PlacementDaemon &&
				row.Placement.Host == daemon {
				roots = append(roots, row.ID)
			}
		}
		reason, endedBy = "daemon_detach:"+daemon, actor.SystemActorID
	default:
		return actorctl.TerminalPlan{}, actorctl.ErrInvalidMutation
	}
	if reason == "" {
		reason = "ended"
	}
	inTree := make(map[actor.ActorID]bool)
	for _, root := range roots {
		if _, ok := byID[root]; ok {
			inTree[root] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for _, row := range rows {
			if !inTree[row.ID] && inTree[row.Sponsor] {
				inTree[row.ID] = true
				changed = true
			}
		}
	}
	plan := terminalStorePlan{}
	s.runMu.RLock()
	for id := range inTree {
		row := byID[id]
		plan.all = append(plan.all, id)
		plan.envelopes = append(plan.envelopes, storespec.CascadeEnvelope{
			Target: id, Reason: reason, EndedBy: endedBy,
		})
		if row.Principal != "" {
			plan.principals = append(plan.principals, row.Principal)
		}
		if _, run := s.runRows[id]; run {
			plan.run = append(plan.run, id)
		} else {
			plan.durable = append(plan.durable, id)
		}
	}
	s.runMu.RUnlock()
	sortActorIDs := func(ids []actor.ActorID) {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	sortActorIDs(plan.all)
	sortActorIDs(plan.durable)
	sortActorIDs(plan.run)
	sort.Strings(plan.principals)
	sort.Slice(plan.envelopes, func(i, j int) bool {
		return plan.envelopes[i].Target < plan.envelopes[j].Target
	})
	return actorctl.TerminalPlan{IDs: append([]actor.ActorID(nil), plan.all...), Opaque: plan}, nil
}

func (s *homeActorStore) CommitTerminal(
	ctx context.Context,
	command actorctl.TerminalCommand,
	resolved actorctl.TerminalPlan,
) (actorctl.ValueCommit[actorctl.TerminalResult], error) {
	plan, ok := resolved.Opaque.(terminalStorePlan)
	if !ok {
		return actorctl.ValueCommit[actorctl.TerminalResult]{}, actorctl.ErrInvalidMutation
	}
	var result actorctl.TerminalResult
	var effects storespec.PostCommitEffects
	switch command.Kind {
	case actorctl.TerminalEnd:
		if len(plan.durable) > 0 {
			if _, err := s.cs.Cascade.EndCascade(ctx, storespec.CascadeBundle{
				IDs: plan.durable, EndedAt: s.now().UnixMilli(), Envelopes: plan.envelopes,
			}); err != nil {
				return actorctl.ValueCommit[actorctl.TerminalResult]{}, err
			}
		}
		result.Ended = append([]actor.ActorID(nil), plan.all...)
	case actorctl.TerminalRemove:
		meta, err := actorCommandMeta(
			command.Remove.Ref,
			command.Remove.Member,
			struct {
				Target           actor.ActorID `json:"target"`
				InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
			}{command.Remove.Target, command.Remove.InitiatorActorID},
		)
		if err != nil {
			return actorctl.ValueCommit[actorctl.TerminalResult]{}, err
		}
		removed, err := s.cs.SysOps.RemoveActor(ctx, storespec.RemoveTx{
			SysOpMeta:        meta,
			Target:           command.Remove.Target,
			InitiatorActorID: command.Remove.InitiatorActorID,
			Reason:           "system_remove",
			DurableIDs:       plan.durable,
			Envelopes:        plan.envelopes,
		})
		if err != nil {
			return actorctl.ValueCommit[actorctl.TerminalResult]{}, err
		}
		result.Remove = channel.RemoveResult{Removed: removed.Removed}
	case actorctl.TerminalDetachDaemon:
		meta, err := systemMeta(command.Detach.Ref, command.Detach)
		if err != nil {
			return actorctl.ValueCommit[actorctl.TerminalResult]{}, err
		}
		detached, err := s.cs.SysOps.DetachDaemon(ctx, storespec.DetachTx{
			SysOpMeta:  meta,
			DaemonID:   storespec.DaemonID(command.Detach.DaemonID),
			DurableIDs: plan.durable,
			AllIDs:     plan.all,
			Envelopes:  plan.envelopes,
		})
		if err != nil {
			return actorctl.ValueCommit[actorctl.TerminalResult]{}, err
		}
		result.Detach = channel.BindingResult{
			Bound: detached.Bound, ClearedInstances: detached.ClearedInstances,
		}
		effects = detached.Effects
	default:
		return actorctl.ValueCommit[actorctl.TerminalResult]{}, actorctl.ErrInvalidMutation
	}
	s.runMu.Lock()
	for _, id := range plan.run {
		delete(s.runRows, id)
	}
	s.runMu.Unlock()
	effects.Principals = append(effects.Principals, plan.principals...)
	return actorctl.ValueCommit[actorctl.TerminalResult]{Result: result, Effects: effects}, nil
}

var _ actorctl.Store = (*homeActorStore)(nil)
