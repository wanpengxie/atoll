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

type controllerTransition[T any] struct {
	Result  T
	Wake    []ActorDefinition
	Ended   []actor.ActorID
	Effects storespec.PostCommitEffects
	Fatal   error
}

func finishTransition[T any](
	a *ChannelActors,
	transition controllerTransition[T],
	err error,
) (T, error) {
	if transition.Fatal != nil {
		a.failStop(transition.Fatal)
	}
	if err != nil {
		return transition.Result, err
	}
	if len(transition.Ended) != 0 {
		a.effects.RunActorsEnded(transition.Ended)
	}
	if len(transition.Wake) != 0 {
		a.wakeAfter(transition.Wake...)
	}
	a.effects.ApplyPostCommit(transition.Effects)
	return transition.Result, nil
}

func (c *Controller) publishNew(ctx context.Context, id actor.ActorID) (ActorDefinition, bool, error) {
	unlock := c.gates.lock(id)
	defer unlock()
	stored, active, err := c.store.LookupActive(ctx, id)
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
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.phase != Running {
		if c.phase == Bootstrapping {
			return ActorDefinition{}, false, ErrBootstrapping
		}
		return ActorDefinition{}, false, ErrClosed
	}
	if _, exists := c.actors[id]; exists {
		return definition, false, nil
	}
	c.actors[id] = ActiveActor{
		Definition: definition,
		Desired:    DesiredState{AttemptKey: key},
	}
	return definition, true, nil
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
	transition, err := a.controller.admit(ctx, request)
	return finishTransition(a, transition, err)
}

func (c *Controller) admit(
	ctx context.Context,
	request AdmitRequest,
) (controllerTransition[AdmitResult], error) {
	commit, err := c.store.Admit(ctx, request)
	if err != nil {
		return controllerTransition[AdmitResult]{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := c.publishNew(ctx, id)
	if err != nil {
		return controllerTransition[AdmitResult]{Result: result}, err
	}
	transition := controllerTransition[AdmitResult]{Result: result, Effects: commit.Effects}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (a *ChannelActors) Introduce(ctx context.Context, request IntroduceRequest) (IntroduceResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return IntroduceResult{}, err
	}
	defer done()
	transition, err := a.controller.introduce(ctx, request)
	return finishTransition(a, transition, err)
}

func (c *Controller) introduce(
	ctx context.Context,
	request IntroduceRequest,
) (controllerTransition[IntroduceResult], error) {
	c.placementGate.Lock()
	defer c.placementGate.Unlock()
	commit, err := c.store.Introduce(ctx, request)
	if err != nil {
		return controllerTransition[IntroduceResult]{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := c.publishNew(ctx, id)
	if err != nil {
		return controllerTransition[IntroduceResult]{Result: result}, err
	}
	transition := controllerTransition[IntroduceResult]{Result: result, Effects: commit.Effects}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (a *ChannelActors) Fork(ctx context.Context, request ForkRequest) (ForkResult, error) {
	done, err := a.beginCommand()
	if err != nil {
		return ForkResult{}, err
	}
	defer done()
	transition, err := a.controller.fork(ctx, request)
	return finishTransition(a, transition, err)
}

func (c *Controller) fork(
	ctx context.Context,
	request ForkRequest,
) (controllerTransition[ForkResult], error) {
	if request.CallerActorID == "" || request.CallerActorID == actor.SystemActorID || request.RequestID == "" {
		return controllerTransition[ForkResult]{}, ErrForkInvalid
	}
	if child, found, lookupErr := c.store.LookupFork(ctx, request.CallerActorID, request.RequestID); lookupErr != nil {
		return controllerTransition[ForkResult]{}, lookupErr
	} else if found {
		return controllerTransition[ForkResult]{Result: ForkResult{ChildActorID: child}}, nil
	}

	c.placementGate.Lock()
	defer c.placementGate.Unlock()

	unlockCaller := c.gates.lock(request.CallerActorID)
	if child, found, lookupErr := c.store.LookupFork(ctx, request.CallerActorID, request.RequestID); lookupErr != nil {
		unlockCaller()
		return controllerTransition[ForkResult]{}, lookupErr
	} else if found {
		unlockCaller()
		return controllerTransition[ForkResult]{Result: ForkResult{ChildActorID: child}}, nil
	}
	if err := c.checkCurrentSnapshot(request.CallerActorID, request.CallerAttempt); err != nil {
		unlockCaller()
		return controllerTransition[ForkResult]{}, err
	}
	parent, ok, err := c.lookup(request.CallerActorID)
	unlockCaller()
	if err != nil {
		return controllerTransition[ForkResult]{}, err
	}
	if !ok {
		return controllerTransition[ForkResult]{}, ErrInactive
	}
	spec, placement, err := normalizeFork(request.Spec, parent.Definition)
	if err != nil {
		return controllerTransition[ForkResult]{}, err
	}
	candidate := freshChildID(request.CallerActorID, spec.NameHint)
	committed, err := c.store.CommitFork(ctx, ForkCommitRequest{
		CallerActorID: request.CallerActorID,
		RequestID:     request.RequestID,
		ChildActorID:  candidate,
		Spec:          spec,
		Placement:     placement,
	})
	if err != nil {
		return controllerTransition[ForkResult]{}, err
	}
	child := committed.ChildActorID
	if child == "" {
		child = committed.Actor.Row.ID
	}
	if child == "" {
		return controllerTransition[ForkResult]{}, ErrForkInvalid
	}
	if err := c.valueEffects.RunActorBorn(child); err != nil {
		return controllerTransition[ForkResult]{Fatal: err}, err
	}
	definition, changed, err := c.publishNew(ctx, child)
	if err != nil {
		return controllerTransition[ForkResult]{Result: ForkResult{ChildActorID: child}}, err
	}
	transition := controllerTransition[ForkResult]{
		Result: ForkResult{ChildActorID: child}, Effects: committed.Effects,
	}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (a *ChannelActors) Restart(ctx context.Context, request RestartRequest) error {
	done, err := a.beginCommand()
	if err != nil {
		return err
	}
	defer done()
	transition, err := a.controller.restart(ctx, request)
	_, err = finishTransition(a, transition, err)
	return err
}

func (c *Controller) restart(
	ctx context.Context,
	request RestartRequest,
) (controllerTransition[struct{}], error) {
	if request.ActorID == actor.SystemActorID {
		return controllerTransition[struct{}]{}, ErrReservedSystem
	}
	unlock := c.gates.lock(request.ActorID)
	commit, err := c.store.Restart(ctx, request)
	if err == nil {
		err = c.publishReplacementLocked(ctx, request.ActorID)
	}
	unlock()
	if err != nil {
		return controllerTransition[struct{}]{}, err
	}
	value, _, _ := c.lookup(request.ActorID)
	return controllerTransition[struct{}]{
		Wake: []ActorDefinition{value.Definition}, Effects: commit.Effects,
	}, nil
}

func (a *ChannelActors) ApplyDeclaration(ctx context.Context, change DeclarationChange) error {
	done, err := a.beginCommand()
	if err != nil {
		return err
	}
	defer done()
	transition, err := a.controller.applyDefinitionChange(ctx, change)
	_, err = finishTransition(a, transition, err)
	return err
}

func (c *Controller) applyDefinitionChange(
	ctx context.Context,
	change DeclarationChange,
) (controllerTransition[struct{}], error) {
	if change.ActorID == actor.SystemActorID {
		return controllerTransition[struct{}]{}, ErrReservedSystem
	}
	c.placementGate.Lock()
	defer c.placementGate.Unlock()
	unlock := c.gates.lock(change.ActorID)
	commit, err := c.store.ApplyDeclaration(ctx, change)
	if err == nil {
		err = c.publishReplacementLocked(ctx, change.ActorID)
	}
	unlock()
	if err != nil {
		return controllerTransition[struct{}]{}, err
	}
	value, _, _ := c.lookup(change.ActorID)
	return controllerTransition[struct{}]{
		Wake: []ActorDefinition{value.Definition}, Effects: commit.Effects,
	}, nil
}

// publishReplacementLocked requires the corresponding control gate.
func (c *Controller) publishReplacementLocked(
	ctx context.Context,
	id actor.ActorID,
) error {
	stored, active, err := c.store.LookupActive(ctx, id)
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
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.phase != Running {
		if c.phase == Bootstrapping {
			return ErrBootstrapping
		}
		return ErrClosed
	}
	_, exists := c.actors[id]
	if !exists {
		return ErrInactive
	}
	key, keyErr := mintAttempt()
	if keyErr != nil {
		return keyErr
	}
	c.actors[id] = ActiveActor{
		Definition: definition,
		Desired:    DesiredState{AttemptKey: key},
	}
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
	transition, err := a.controller.attachDaemon(ctx, request)
	result, err := finishTransition(a, transition, err)
	if err == nil {
		a.effects.PlanPoke(actorhost.ExecutionDomain(request.DaemonID))
	}
	return result, err
}

func (c *Controller) attachDaemon(
	ctx context.Context,
	request AttachDaemonRequest,
) (controllerTransition[AttachDaemonResult], error) {
	c.placementGate.Lock()
	defer c.placementGate.Unlock()
	commit, err := c.store.AttachDaemon(ctx, request)
	return controllerTransition[AttachDaemonResult]{
		Result: commit.Result, Effects: commit.Effects,
	}, err
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
	transition, err := a.controller.terminal(ctx, command)
	result, err := finishTransition(a, transition, err)
	if err == nil && command.Kind == TerminalDetachDaemon {
		a.effects.PlanPoke(actorhost.ExecutionDomain(command.Detach.DaemonID))
	}
	return result, err
}

func (c *Controller) admitLifecycle(id actor.ActorID, key actorhost.AttemptKey) error {
	unlock := c.gates.lock(id)
	defer unlock()
	return c.checkCurrentSnapshot(id, key)
}

func (c *Controller) terminal(
	ctx context.Context,
	command TerminalCommand,
) (controllerTransition[TerminalResult], error) {
	if command.Kind == TerminalEnd && command.End.CallerAttempt != "" {
		if err := c.admitLifecycle(command.End.CallerActorID, command.End.CallerAttempt); err != nil {
			return controllerTransition[TerminalResult]{}, err
		}
	}
	if command.Kind == TerminalDetachDaemon {
		c.placementGate.Lock()
		defer c.placementGate.Unlock()
	}
	for {
		beforeRows, rowsErr := c.activeRows()
		if rowsErr != nil {
			return controllerTransition[TerminalResult]{}, rowsErr
		}
		before, err := c.store.ResolveTerminal(ctx, command, beforeRows)
		if err != nil {
			return controllerTransition[TerminalResult]{}, err
		}
		before.IDs = canonicalActorIDs(before.IDs)
		unlock := c.gates.lockActorSet(before.IDs)
		afterRows, rowsErr := c.activeRows()
		if rowsErr != nil {
			unlock()
			return controllerTransition[TerminalResult]{}, rowsErr
		}
		after, err := c.store.ResolveTerminal(ctx, command, afterRows)
		if err != nil {
			unlock()
			return controllerTransition[TerminalResult]{}, err
		}
		after.IDs = canonicalActorIDs(after.IDs)
		if !slices.Equal(before.IDs, after.IDs) {
			unlock()
			continue
		}
		commit, err := c.store.CommitTerminal(ctx, command, after)
		if err != nil {
			unlock()
			return controllerTransition[TerminalResult]{}, err
		}
		definitions := make([]ActorDefinition, 0, len(after.IDs))
		for _, id := range after.IDs {
			if value, exists, lookupErr := c.lookup(id); lookupErr == nil && exists {
				definitions = append(definitions, value.Definition)
			}
		}
		c.delete(after.IDs)
		unlock()
		return controllerTransition[TerminalResult]{
			Result: commit.Result, Wake: definitions, Ended: after.IDs, Effects: commit.Effects,
		}, nil
	}
}

// Keep errors.Is useful when adapters translate a closed command surface.
func isUnavailable(err error) bool {
	return errors.Is(err, ErrClosed) || errors.Is(err, ErrChannelClosing)
}

var _ = storespec.ErrActorNotFound
