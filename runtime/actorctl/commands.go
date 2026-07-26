package actorctl

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var ErrEndForbidden = errors.New("actorctl: only the target itself or the system face may end an actor")

// Admit is the human birth command. Its whole ledger change happens inside the
// ledger lock: durable insert first (fallible), publication after (infallible).
func (c *Controller) Admit(
	ctx context.Context,
	request AdmitRequest,
) (Transition[AdmitResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[AdmitResult]{}, err
	}
	defer done()

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[AdmitResult]{}, err
	}
	if request.Principal == "" {
		return Transition[AdmitResult]{}, ErrInvalidMutation
	}
	// Every fallible step happens before the change settles: the AttemptKey is
	// pre-minted so publication after the commit cannot fail.
	key, err := mintAttempt()
	if err != nil {
		return Transition[AdmitResult]{}, err
	}
	record, err := c.store.Insert(ctx, storespec.ActorDraft{
		Kind:       actor.KindHuman,
		Principal:  request.Principal,
		CreatedAt:  c.nowMs(),
		Definition: storespec.ActorDefinition{Class: humanClass},
		Placement:  storespec.NewServerPlacement(),
	})
	if err != nil {
		return Transition[AdmitResult]{}, err
	}
	created := c.publishLocked(record, key)
	transition := Transition[AdmitResult]{
		Result: channel.AdmitResult{ActorID: record.ID, Created: created},
	}
	if created {
		transition.Reconcile.add(record.Placement)
	}
	return transition, nil
}

// humanClass is the built-in class every admitted human runs.
const humanClass = "human"

// Introduce is the declaration birth command. Every business decision
// (declaration resolution, visibility, placement host) already happened at the
// Platform door; this command is mechanical.
func (c *Controller) Introduce(
	ctx context.Context,
	request IntroduceRequest,
) (Transition[channel.IntroduceResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	defer done()

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	if request.DeclID == "" || request.Definition.Class == "" {
		return Transition[channel.IntroduceResult]{}, ErrInvalidMutation
	}
	key, err := mintAttempt()
	if err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	record, err := c.store.Insert(ctx, storespec.ActorDraft{
		Kind:         request.Kind,
		SourceDeclID: request.DeclID,
		CreatedAt:    c.nowMs(),
		Definition:   request.Definition,
		Placement:    request.Placement,
	})
	if err != nil {
		return Transition[channel.IntroduceResult]{}, err
	}
	created := c.publishLocked(record, key)
	transition := Transition[channel.IntroduceResult]{
		Result: channel.IntroduceResult{ActorID: record.ID, Created: created},
	}
	if created {
		transition.Reconcile.add(record.Placement)
	}
	return transition, nil
}

// Restart is a pure value command: it changes no record, so it has no store
// verb. It is edge-triggered — every successful call is one new term.
func (c *Controller) Restart(
	_ context.Context,
	request RestartRequest,
) (Transition[struct{}], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	defer done()

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[struct{}]{}, err
	}
	value, ok := c.actors[request.ActorID]
	if !ok {
		return Transition[struct{}]{}, ErrInactive
	}
	key, err := mintAttempt()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	value.Attempt = key
	c.actors[request.ActorID] = value
	transition := Transition[struct{}]{}
	transition.Reconcile.add(value.Record.Placement)
	return transition, nil
}

// ApplyDeclaration overwrites one record's definition and mints a new term
// (changing the config IS a new term). An equal definition is the one and only
// home of the equality short-circuit: no row write, no new term.
func (c *Controller) ApplyDeclaration(
	ctx context.Context,
	change DeclarationChange,
) (Transition[struct{}], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	defer done()

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[struct{}]{}, err
	}
	value, ok := c.actors[change.ActorID]
	if !ok {
		return Transition[struct{}]{}, ErrInactive
	}
	if value.Record.Definition.Equal(change.Definition) {
		return Transition[struct{}]{}, nil
	}
	key, err := mintAttempt()
	if err != nil {
		return Transition[struct{}]{}, err
	}
	record, err := c.store.UpdateDefinition(ctx, change.ActorID, change.Definition)
	if err != nil {
		return Transition[struct{}]{}, err
	}
	c.actors[change.ActorID] = managedActor{Record: record, Attempt: key}
	transition := Transition[struct{}]{}
	transition.Reconcile.add(record.Placement)
	return transition, nil
}

func (c *Controller) End(
	ctx context.Context,
	request EndRequest,
) (Transition[EndResult], error) {
	transition, err := c.Terminal(ctx, TerminalCommand{Kind: TerminalEnd, End: request})
	return Transition[EndResult]{
		Result:    EndResult{Ended: transition.Result.Ended},
		Ended:     transition.Ended,
		Reconcile: transition.Reconcile,
	}, err
}

func (c *Controller) Remove(
	ctx context.Context,
	request RemoveRequest,
) (Transition[channel.RemoveResult], error) {
	transition, err := c.Terminal(ctx, TerminalCommand{Kind: TerminalRemove, Remove: request})
	return Transition[channel.RemoveResult]{
		Result:    transition.Result.Remove,
		Ended:     transition.Ended,
		Reconcile: transition.Reconcile,
	}, err
}

// Terminal is the whole termination command. The terminal set is exactly the
// explicit target — there is no lineage cascade, no plan, no pre-classification
// and no third beat.
func (c *Controller) Terminal(
	ctx context.Context,
	command TerminalCommand,
) (Transition[TerminalResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[TerminalResult]{}, err
	}
	defer done()

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[TerminalResult]{}, err
	}

	var targets []actor.ActorID
	var result TerminalResult
	switch command.Kind {
	case TerminalEnd:
		request := command.End
		if request.CallerAttempt != "" {
			if err := c.checkCurrentLocked(request.CallerActorID, request.CallerAttempt); err != nil {
				return Transition[TerminalResult]{}, err
			}
		}
		caller := request.CallerActorID
		if caller != "" && caller != actor.SystemActorID && caller != request.Target {
			return Transition[TerminalResult]{}, ErrEndForbidden
		}
		if _, active := c.actors[request.Target]; active {
			targets = []actor.ActorID{request.Target}
		}
	case TerminalRemove:
		request := command.Remove
		if request.InitiatorActorID == "" {
			return Transition[TerminalResult]{}, ErrInvalidMutation
		}
		if _, member := c.actors[request.InitiatorActorID]; !member {
			return Transition[TerminalResult]{}, ErrInactive
		}
		if _, active := c.actors[request.Target]; active {
			targets = []actor.ActorID{request.Target}
		}
	default:
		return Transition[TerminalResult]{}, ErrInvalidMutation
	}

	if len(targets) == 0 {
		if command.Kind == TerminalEnd {
			return Transition[TerminalResult]{}, nil
		}
		return Transition[TerminalResult]{}, nil
	}
	if err := c.store.Deregister(ctx, targets); err != nil {
		return Transition[TerminalResult]{}, err
	}
	transition := Transition[TerminalResult]{}
	for _, id := range targets {
		transition.Reconcile.add(c.actors[id].Record.Placement)
		delete(c.actors, id)
	}
	switch command.Kind {
	case TerminalEnd:
		result.Ended = append([]actor.ActorID(nil), targets...)
	case TerminalRemove:
		result.Remove = channel.RemoveResult{Removed: append([]actor.ActorID(nil), targets...)}
	}
	transition.Result = result
	transition.Ended = append([]actor.ActorID(nil), targets...)
	return transition, nil
}

// publishLocked is the infallible publication half of a birth command: plain
// memory assignment with a pre-minted key. A record already in the ledger (a
// replayed birth resolved by semantic key) publishes nothing.
func (c *Controller) publishLocked(
	record storespec.ActorRecord,
	key actorhost.AttemptKey,
) bool {
	if _, exists := c.actors[record.ID]; exists {
		return false
	}
	c.actors[record.ID] = managedActor{Record: record, Attempt: key}
	return true
}
