package actorctl

import (
	"context"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// managedLifecycle is minted only by the ChannelActors-owned Host builder. It
// shares the one value-ledger gate welded to the other four managed arms, so a
// lifecycle call clears the exact same logical-then-physical admission as a Pen
// / Access / State / Schedule call.
type managedLifecycle struct {
	actors *ChannelActors
	id     actor.ActorID
	key    actorhost.AttemptKey
	gate   *managedInvocation
}

func (h managedLifecycle) admitInvocation() error {
	if h.actors == nil || h.gate == nil {
		return ErrStaleAttempt
	}
	return h.gate.admit()
}

func (h managedLifecycle) Fork(
	ctx context.Context,
	requestID message.ID,
	spec actorcaps.ForkSpec,
) (actor.ActorID, error) {
	if err := h.admitInvocation(); err != nil {
		return "", err
	}
	result, err := h.actors.Fork(ctx, ForkRequest{
		CallerActorID: h.id,
		CallerAttempt: h.key,
		RequestID:     requestID,
		Spec:          spec,
	})
	return result.ChildActorID, err
}

func (h managedLifecycle) RequestIdle(ctx context.Context) error {
	if err := h.admitInvocation(); err != nil {
		return err
	}
	return h.actors.requestIdle(ctx, h.id, h.key)
}

func (h managedLifecycle) EndSelf(ctx context.Context, request actorcaps.EndSelfRequest) error {
	if err := h.admitInvocation(); err != nil {
		return err
	}
	_, err := h.actors.End(ctx, EndRequest{
		CallerActorID: h.id,
		CallerAttempt: h.key,
		Target:        h.id,
		Reason:        request.Reason,
	})
	return err
}

var _ actorcaps.LifecycleHandle = managedLifecycle{}
