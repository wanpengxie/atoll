package actorctl

import (
	"context"
	"errors"
	"slices"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Transition is the committed change set returned by one Controller
// operation. Platform owns every cross-organ tail represented by these facts.
type Transition[T any] struct {
	Result  T
	Wake    []ActorDefinition
	Ended   []actor.ActorID
	Effects storespec.PostCommitEffects
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

func (c *Controller) Admit(
	ctx context.Context,
	request AdmitRequest,
) (Transition[AdmitResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[AdmitResult]{}, err
	}
	defer done()
	return c.admit(ctx, request)
}

func (c *Controller) admit(
	ctx context.Context,
	request AdmitRequest,
) (Transition[AdmitResult], error) {
	commit, err := c.store.Admit(ctx, request)
	if err != nil {
		return Transition[AdmitResult]{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := c.publishNew(ctx, id)
	if err != nil {
		return Transition[AdmitResult]{Result: result}, err
	}
	transition := Transition[AdmitResult]{Result: result, Effects: commit.Effects}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (c *Controller) Introduce(
	ctx context.Context,
	request IntroduceRequest,
) (Transition[channel.IntroduceResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	defer done()
	return c.introduce(ctx, request)
}

func (c *Controller) introduce(
	ctx context.Context,
	request IntroduceRequest,
) (Transition[channel.IntroduceResult], error) {
	c.placementGate.Lock()
	defer c.placementGate.Unlock()
	commit, err := c.store.Introduce(ctx, request)
	if err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	stored, result := commit.Actor, commit.Result
	id := result.ActorID
	if id == "" {
		id = stored.Row.ID
	}
	definition, changed, err := c.publishNew(ctx, id)
	if err != nil {
		return Transition[channel.IntroduceResult]{Result: result}, err
	}
	transition := Transition[channel.IntroduceResult]{Result: result, Effects: commit.Effects}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (c *Controller) Fork(
	ctx context.Context,
	request ForkRequest,
) (Transition[ForkResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	defer done()
	return c.fork(ctx, request)
}

func (c *Controller) fork(
	ctx context.Context,
	request ForkRequest,
) (Transition[ForkResult], error) {
	if request.CallerActorID == "" || request.CallerActorID == actor.SystemActorID || request.RequestID == "" {
		return Transition[ForkResult]{}, ErrForkInvalid
	}
	if child, found, lookupErr := c.store.LookupFork(ctx, request.CallerActorID, request.RequestID); lookupErr != nil {
		return Transition[ForkResult]{}, lookupErr
	} else if found {
		return Transition[ForkResult]{Result: ForkResult{ChildActorID: child}}, nil
	}

	c.placementGate.Lock()
	defer c.placementGate.Unlock()

	admission, err := c.admitFork(ctx, request)
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	if admission.found {
		return Transition[ForkResult]{
			Result: ForkResult{ChildActorID: admission.child},
		}, nil
	}
	spec, placement := admission.spec, admission.placement
	candidate := freshChildID(request.CallerActorID, spec.NameHint)
	committed, err := c.store.CommitFork(ctx, ForkCommitRequest{
		CallerActorID: request.CallerActorID,
		RequestID:     request.RequestID,
		ChildActorID:  candidate,
		Spec:          spec,
		Placement:     placement,
	})
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	child := committed.ChildActorID
	if child == "" {
		child = committed.Actor.Row.ID
	}
	if child == "" {
		return Transition[ForkResult]{}, ErrForkInvalid
	}
	definition, changed, err := c.publishNew(ctx, child)
	if err != nil {
		return Transition[ForkResult]{Result: ForkResult{ChildActorID: child}}, err
	}
	transition := Transition[ForkResult]{
		Result: ForkResult{ChildActorID: child}, Effects: committed.Effects,
	}
	if changed {
		transition.Wake = []ActorDefinition{definition}
	}
	return transition, nil
}

func (c *Controller) Restart(
	ctx context.Context,
	request RestartRequest,
) (Transition[struct{}], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	defer done()
	return c.restart(ctx, request)
}

func (c *Controller) restart(
	ctx context.Context,
	request RestartRequest,
) (Transition[struct{}], error) {
	if request.ActorID == actor.SystemActorID {
		return Transition[struct{}]{}, ErrReservedSystem
	}
	unlock := c.gates.lock(request.ActorID)
	commit, err := c.store.Restart(ctx, request)
	if err == nil {
		err = c.publishReplacementLocked(ctx, request.ActorID)
	}
	unlock()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	value, _, _ := c.Lookup(request.ActorID)
	return Transition[struct{}]{
		Wake: []ActorDefinition{value.Definition}, Effects: commit.Effects,
	}, nil
}

func (c *Controller) ApplyDeclaration(
	ctx context.Context,
	change DeclarationChange,
) (Transition[struct{}], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	defer done()
	return c.applyDefinitionChange(ctx, change)
}

func (c *Controller) applyDefinitionChange(
	ctx context.Context,
	change DeclarationChange,
) (Transition[struct{}], error) {
	if change.ActorID == actor.SystemActorID {
		return Transition[struct{}]{}, ErrReservedSystem
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
		return Transition[struct{}]{}, err
	}
	value, _, _ := c.Lookup(change.ActorID)
	return Transition[struct{}]{
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

func (c *Controller) AttachDaemon(
	ctx context.Context,
	request channel.DaemonRequest,
) (Transition[channel.BindingResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[channel.BindingResult]{}, err
	}
	defer done()
	return c.attachDaemon(ctx, request)
}

func (c *Controller) attachDaemon(
	ctx context.Context,
	request channel.DaemonRequest,
) (Transition[channel.BindingResult], error) {
	c.placementGate.Lock()
	defer c.placementGate.Unlock()
	commit, err := c.store.AttachDaemon(ctx, request)
	return Transition[channel.BindingResult]{
		Result: commit.Result, Effects: commit.Effects,
	}, err
}

func (c *Controller) End(
	ctx context.Context,
	request EndRequest,
) (Transition[EndResult], error) {
	transition, err := c.Terminal(ctx, TerminalCommand{Kind: TerminalEnd, End: request})
	return Transition[EndResult]{
		Result:  EndResult{Ended: transition.Result.Ended},
		Wake:    transition.Wake,
		Ended:   transition.Ended,
		Effects: transition.Effects,
	}, err
}

func (c *Controller) Remove(
	ctx context.Context,
	request RemoveRequest,
) (Transition[channel.RemoveResult], error) {
	transition, err := c.Terminal(ctx, TerminalCommand{Kind: TerminalRemove, Remove: request})
	return Transition[channel.RemoveResult]{
		Result:  transition.Result.Remove,
		Wake:    transition.Wake,
		Ended:   transition.Ended,
		Effects: transition.Effects,
	}, err
}

func (c *Controller) DetachDaemon(
	ctx context.Context,
	request channel.DaemonRequest,
) (Transition[channel.BindingResult], error) {
	transition, err := c.Terminal(
		ctx,
		TerminalCommand{Kind: TerminalDetachDaemon, Detach: request},
	)
	return Transition[channel.BindingResult]{
		Result:  transition.Result.Detach,
		Wake:    transition.Wake,
		Ended:   transition.Ended,
		Effects: transition.Effects,
	}, err
}

func (c *Controller) Terminal(
	ctx context.Context,
	command TerminalCommand,
) (Transition[TerminalResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[TerminalResult]{}, err
	}
	defer done()
	return c.terminal(ctx, command)
}

func (c *Controller) terminal(
	ctx context.Context,
	command TerminalCommand,
) (Transition[TerminalResult], error) {
	if command.Kind == TerminalDetachDaemon {
		c.placementGate.Lock()
		defer c.placementGate.Unlock()
	}
	for {
		beforeRows, rowsErr := c.ActiveRows()
		if rowsErr != nil {
			return Transition[TerminalResult]{}, rowsErr
		}
		before, err := c.store.ResolveTerminal(ctx, command, beforeRows)
		if err != nil {
			return Transition[TerminalResult]{}, err
		}
		before.IDs = canonicalActorIDs(before.IDs)
		lockIDs := append([]actor.ActorID(nil), before.IDs...)
		if command.Kind == TerminalEnd && command.End.CallerAttempt != "" {
			lockIDs = append(lockIDs, command.End.CallerActorID)
		}
		unlock := c.gates.lockActorSet(lockIDs)
		afterRows, rowsErr := c.ActiveRows()
		if rowsErr != nil {
			unlock()
			return Transition[TerminalResult]{}, rowsErr
		}
		after, err := c.store.ResolveTerminal(ctx, command, afterRows)
		if err != nil {
			unlock()
			return Transition[TerminalResult]{}, err
		}
		after.IDs = canonicalActorIDs(after.IDs)
		if !slices.Equal(before.IDs, after.IDs) {
			unlock()
			continue
		}
		if command.Kind == TerminalEnd && command.End.CallerAttempt != "" {
			if err := c.checkCurrentSnapshot(
				command.End.CallerActorID,
				command.End.CallerAttempt,
			); err != nil {
				unlock()
				return Transition[TerminalResult]{}, err
			}
		}
		commit, err := c.store.CommitTerminal(ctx, command, after)
		if err != nil {
			unlock()
			return Transition[TerminalResult]{}, err
		}
		definitions := make([]ActorDefinition, 0, len(after.IDs))
		for _, id := range after.IDs {
			if value, exists, lookupErr := c.Lookup(id); lookupErr == nil && exists {
				definitions = append(definitions, value.Definition)
			}
		}
		c.delete(after.IDs)
		unlock()
		return Transition[TerminalResult]{
			Result: commit.Result, Wake: definitions, Ended: after.IDs, Effects: commit.Effects,
		}, nil
	}
}

// Keep errors.Is useful when adapters translate a closed command surface.
func isUnavailable(err error) bool {
	return errors.Is(err, ErrClosed) || errors.Is(err, ErrChannelClosing)
}

var _ = storespec.ErrActorNotFound
