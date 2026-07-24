package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
)

// WithStopFirst wraps one Actor so before runs exactly at the beginning of its
// Stop hook. The wrapper preserves optional Start, DownReporter, and
// RequestCanceller behavior. It is an atomic execution adapter, not an actor
// manager: it owns no identity, collection, or lifecycle decision.
func WithStopFirst(impl Actor, before func()) Actor {
	if impl == nil {
		return nil
	}
	return &stopFirstActor{impl: impl, before: before}
}

type stopFirstActor struct {
	impl   Actor
	before func()
}

func (a *stopFirstActor) Receive(ctx context.Context, env *message.Envelope) error {
	return a.impl.Receive(ctx, env)
}

func (a *stopFirstActor) Start(ctx context.Context, self ActorContext) error {
	if starter, ok := a.impl.(Starter); ok {
		return starter.Start(ctx, self)
	}
	return nil
}

func (a *stopFirstActor) Stop(ctx context.Context) error {
	if a.before != nil {
		a.before()
	}
	if stopper, ok := a.impl.(Stopper); ok {
		return stopper.Stop(ctx)
	}
	return nil
}

func (a *stopFirstActor) Dying() <-chan error {
	if reporter, ok := a.impl.(DownReporter); ok {
		return reporter.Dying()
	}
	return nil
}

func (a *stopFirstActor) CancelRequest(id message.ID) {
	if canceller, ok := a.impl.(RequestCanceller); ok {
		canceller.CancelRequest(id)
	}
}
