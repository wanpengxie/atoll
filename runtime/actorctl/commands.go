package actorctl

import (
	"context"
	"errors"
	"slices"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (a *ChannelActors) beginCommand() (func(), error) {
	if a == nil {
		return nil, ErrClosed
	}
	return a.owner.begin()
}

func (a *ChannelActors) publishNew(ctx context.Context, id actor.ActorID) (ActorDefinition, bool, error) {
	unlock := a.controller.gates.lock(id)
	defer unlock()
	stored, active, err := a.store.LookupActive(ctx, id)
	if err != nil || !active {
		return ActorDefinition{}, false, err
	}
	definition, err := definitionFromStored(stored)
	if err != nil {
		return ActorDefinition{}, false, err
	}
	key, err := mintAttempt()
	if err != nil {
		return ActorDefinition{}, false, err
	}
	a.controller.stateMu.Lock()
	defer a.controller.stateMu.Unlock()
	if a.controller.phase != Running {
		if a.controller.phase == Bootstrapping {
			return ActorDefinition{}, false, ErrBootstrapping
		}
		return ActorDefinition{}, false, ErrClosed
	}
	if _, exists := a.controller.actors[id]; exists {
		return definition, false, nil
	}
	a.controller.actors[id] = ActiveActor{
		Definition: definition,
		Desired:    DesiredState{Kind: DesiredRun, AttemptKey: key},
	}
	return definition, true, nil
}

func (a *ChannelActors) publishReplacement(
	ctx context.Context,
	id actor.ActorID,
	forceRun bool,
) (ActorDefinition, error) {
	unlock := a.controller.gates.lock(id)
	defer unlock()
	stored, active, err := a.store.LookupActive(ctx, id)
	if err != nil {
		return ActorDefinition{}, err
	}
	if !active {
		return ActorDefinition{}, ErrInactive
	}
	definition, err := definitionFromStored(stored)
	if err != nil {
		return ActorDefinition{}, err
	}
	a.controller.stateMu.Lock()
	defer a.controller.stateMu.Unlock()
	if a.controller.phase != Running {
		if a.controller.phase == Bootstrapping {
			return ActorDefinition{}, ErrBootstrapping
		}
		return ActorDefinition{}, ErrClosed
	}
	current, exists := a.controller.actors[id]
	if !exists {
		return ActorDefinition{}, ErrInactive
	}
	desired := current.Desired
	if forceRun || desired.Kind == DesiredRun {
		key, keyErr := mintAttempt()
		if keyErr != nil {
			return ActorDefinition{}, keyErr
		}
		desired = DesiredState{Kind: DesiredRun, AttemptKey: key}
	}
	a.controller.actors[id] = ActiveActor{Definition: definition, Desired: desired}
	return definition, nil
}

func (a *ChannelActors) wakeAfter(definitions ...ActorDefinition) {
	a.pokeServerDesired()
	for _, definition := range definitions {
		a.wakeDefinition(definition)
	}
}

func (a *ChannelActors) Admit(ctx context.Context, request AdmitRequest) (AdmitResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return AdmitResult{}, err
	}
	defer done()
	commit, err := a.store.Admit(ctx, request)
	if err != nil {
		return AdmitResult{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := a.publishNew(ctx, id)
	if err != nil {
		return AdmitResult{}, err
	}
	if changed {
		a.wakeAfter(definition)
	}
	a.effects.ApplyPostCommit(commit.Effects)
	return result, nil
}

func (a *ChannelActors) Introduce(ctx context.Context, request IntroduceRequest) (IntroduceResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return IntroduceResult{}, err
	}
	defer done()
	a.controller.placementGate.Lock()
	defer a.controller.placementGate.Unlock()
	commit, err := a.store.Introduce(ctx, request)
	if err != nil {
		return IntroduceResult{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := a.publishNew(ctx, id)
	if err != nil {
		return IntroduceResult{}, err
	}
	if changed {
		a.wakeAfter(definition)
	}
	a.effects.ApplyPostCommit(commit.Effects)
	return result, nil
}

func (a *ChannelActors) Fork(ctx context.Context, request ForkRequest) (ForkResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return ForkResult{}, err
	}
	defer done()
	if request.CallerActorID == "" || request.CallerActorID == actor.SystemActorID || request.RequestID == "" {
		return ForkResult{}, ErrForkInvalid
	}
	if child, found, lookupErr := a.store.LookupFork(ctx, request.CallerActorID, request.RequestID); lookupErr != nil {
		return ForkResult{}, lookupErr
	} else if found {
		return ForkResult{ChildActorID: child}, nil
	}

	a.controller.placementGate.Lock()
	defer a.controller.placementGate.Unlock()

	unlockCaller := a.controller.gates.lock(request.CallerActorID)
	if child, found, lookupErr := a.store.LookupFork(ctx, request.CallerActorID, request.RequestID); lookupErr != nil {
		unlockCaller()
		return ForkResult{}, lookupErr
	} else if found {
		unlockCaller()
		return ForkResult{ChildActorID: child}, nil
	}
	if err := a.controller.isCurrent(request.CallerActorID, request.CallerAttempt); err != nil {
		unlockCaller()
		return ForkResult{}, err
	}
	parent, ok, err := a.controller.lookup(request.CallerActorID)
	unlockCaller()
	if err != nil {
		return ForkResult{}, err
	}
	if !ok {
		return ForkResult{}, ErrInactive
	}
	spec, placement, err := normalizeFork(request.Spec, parent.Definition)
	if err != nil {
		return ForkResult{}, err
	}
	candidate := freshChildID(request.CallerActorID, spec.NameHint)
	committed, err := a.store.CommitFork(ctx, ForkCommitRequest{
		CallerActorID: request.CallerActorID,
		RequestID:     request.RequestID,
		ChildActorID:  candidate,
		Spec:          spec,
		Placement:     placement,
	})
	if err != nil {
		return ForkResult{}, err
	}
	child := committed.ChildActorID
	if child == "" {
		child = committed.Actor.Row.ID
	}
	if child == "" {
		return ForkResult{}, ErrForkInvalid
	}
	if err := a.effects.RunActorBorn(child); err != nil {
		a.failStop(err)
		return ForkResult{}, err
	}
	definition, changed, err := a.publishNew(ctx, child)
	if err != nil {
		return ForkResult{}, err
	}
	if changed {
		a.wakeAfter(definition)
	}
	a.effects.ApplyPostCommit(committed.Effects)
	return ForkResult{ChildActorID: child}, nil
}

func (a *ChannelActors) Restart(ctx context.Context, request RestartRequest) error {
	done, err := a.beginCommand()
	if err != nil {
		return err
	}
	defer done()
	if request.ActorID == actor.SystemActorID {
		return ErrReservedSystem
	}
	unlock := a.controller.gates.lock(request.ActorID)
	commit, err := a.store.Restart(ctx, request)
	if err == nil {
		err = a.publishReplacementLocked(ctx, request.ActorID, true)
	}
	unlock()
	if err != nil {
		return err
	}
	value, _, _ := a.controller.lookup(request.ActorID)
	a.wakeAfter(value.Definition)
	a.effects.ApplyPostCommit(commit.Effects)
	return nil
}

func (a *ChannelActors) ApplyDeclaration(ctx context.Context, change DeclarationChange) error {
	done, err := a.beginCommand()
	if err != nil {
		return err
	}
	defer done()
	if change.ActorID == actor.SystemActorID {
		return ErrReservedSystem
	}
	a.controller.placementGate.Lock()
	defer a.controller.placementGate.Unlock()
	unlock := a.controller.gates.lock(change.ActorID)
	commit, err := a.store.ApplyDeclaration(ctx, change)
	if err == nil {
		err = a.publishReplacementLocked(ctx, change.ActorID, false)
	}
	unlock()
	if err != nil {
		return err
	}
	value, _, _ := a.controller.lookup(change.ActorID)
	a.wakeAfter(value.Definition)
	a.effects.ApplyPostCommit(commit.Effects)
	return nil
}

// publishReplacementLocked requires the corresponding control gate.
func (a *ChannelActors) publishReplacementLocked(
	ctx context.Context,
	id actor.ActorID,
	forceRun bool,
) error {
	stored, active, err := a.store.LookupActive(ctx, id)
	if err != nil {
		return err
	}
	if !active {
		return ErrInactive
	}
	definition, err := definitionFromStored(stored)
	if err != nil {
		return err
	}
	a.controller.stateMu.Lock()
	defer a.controller.stateMu.Unlock()
	if a.controller.phase != Running {
		if a.controller.phase == Bootstrapping {
			return ErrBootstrapping
		}
		return ErrClosed
	}
	current, exists := a.controller.actors[id]
	if !exists {
		return ErrInactive
	}
	desired := current.Desired
	if forceRun || desired.Kind == DesiredRun {
		key, keyErr := mintAttempt()
		if keyErr != nil {
			return keyErr
		}
		desired = DesiredState{Kind: DesiredRun, AttemptKey: key}
	}
	a.controller.actors[id] = ActiveActor{Definition: definition, Desired: desired}
	return nil
}

func (a *ChannelActors) AttachDaemon(
	ctx context.Context,
	request AttachDaemonRequest,
) (AttachDaemonResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return AttachDaemonResult{}, err
	}
	defer done()
	a.controller.placementGate.Lock()
	defer a.controller.placementGate.Unlock()
	commit, err := a.store.AttachDaemon(ctx, request)
	if err == nil {
		a.effects.PlanPoke(actorhost.ExecutionDomain(request.DaemonID))
		a.effects.ApplyPostCommit(commit.Effects)
	}
	return commit.Result, err
}

func (a *ChannelActors) End(ctx context.Context, request EndRequest) (EndResult, error) {
	result, err := a.terminal(ctx, TerminalCommand{Kind: TerminalEnd, End: request})
	return EndResult{Ended: result.Ended}, err
}

func (a *ChannelActors) Remove(ctx context.Context, request RemoveRequest) (RemoveResult, error) {
	result, err := a.terminal(ctx, TerminalCommand{Kind: TerminalRemove, Remove: request})
	return result.Remove, err
}

func (a *ChannelActors) DetachDaemon(
	ctx context.Context,
	request DetachDaemonRequest,
) (DetachDaemonResult, error) {
	result, err := a.terminal(ctx, TerminalCommand{Kind: TerminalDetachDaemon, Detach: request})
	return result.Detach, err
}

func (a *ChannelActors) terminal(ctx context.Context, command TerminalCommand) (TerminalResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return TerminalResult{}, err
	}
	defer done()
	if command.Kind == TerminalEnd && command.End.CallerAttempt != "" {
		if err := a.controller.isCurrent(command.End.CallerActorID, command.End.CallerAttempt); err != nil {
			return TerminalResult{}, err
		}
	}
	if command.Kind == TerminalDetachDaemon {
		a.controller.placementGate.Lock()
		defer a.controller.placementGate.Unlock()
	}
	for {
		beforeRows, rowsErr := a.listActiveRows()
		if rowsErr != nil {
			return TerminalResult{}, rowsErr
		}
		before, err := a.store.ResolveTerminal(ctx, command, beforeRows)
		if err != nil {
			return TerminalResult{}, err
		}
		before.IDs = canonicalActorIDs(before.IDs)
		unlock := a.controller.gates.lockActorSet(before.IDs)
		afterRows, rowsErr := a.listActiveRows()
		if rowsErr != nil {
			unlock()
			return TerminalResult{}, rowsErr
		}
		after, err := a.store.ResolveTerminal(ctx, command, afterRows)
		if err != nil {
			unlock()
			return TerminalResult{}, err
		}
		after.IDs = canonicalActorIDs(after.IDs)
		if !slices.Equal(before.IDs, after.IDs) {
			unlock()
			continue
		}
		commit, err := a.store.CommitTerminal(ctx, command, after)
		if err != nil {
			unlock()
			return TerminalResult{}, err
		}
		definitions := make([]ActorDefinition, 0, len(after.IDs))
		for _, id := range after.IDs {
			if value, exists, lookupErr := a.controller.lookup(id); lookupErr == nil && exists {
				definitions = append(definitions, value.Definition)
			}
		}
		a.controller.delete(after.IDs)
		a.effects.RunActorsEnded(after.IDs)
		unlock()
		a.wakeAfter(definitions...)
		if command.Kind == TerminalDetachDaemon {
			a.effects.PlanPoke(actorhost.ExecutionDomain(command.Detach.DaemonID))
		}
		a.effects.ApplyPostCommit(commit.Effects)
		return commit.Result, nil
	}
}

func (a *ChannelActors) requestIdle(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
) error {
	done, err := a.beginCommand()
	if err != nil {
		return err
	}
	defer done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	unlock := a.controller.gates.lock(id)
	a.controller.stateMu.Lock()
	if a.controller.phase != Running {
		if a.controller.phase == Bootstrapping {
			err = ErrBootstrapping
		} else {
			err = ErrClosed
		}
	} else {
		current, exists := a.controller.actors[id]
		switch {
		case !exists:
			err = ErrInactive
		case current.Desired.Kind != DesiredRun || current.Desired.AttemptKey != key:
			err = ErrStaleAttempt
		default:
			current.Desired = DesiredState{Kind: DesiredDormant}
			a.controller.actors[id] = current
		}
	}
	a.controller.stateMu.Unlock()
	unlock()
	if err != nil {
		return err
	}
	a.pokeServerDesired()
	return nil
}

// Keep errors.Is useful when adapters translate a closed command surface.
func isUnavailable(err error) bool {
	return errors.Is(err, ErrClosed) || errors.Is(err, ErrChannelClosing)
}

var _ = storespec.ErrActorNotFound
