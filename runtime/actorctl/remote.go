package actorctl

import (
	"context"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// AuthorizeAttach is the Server-side A/G + placement gate for one exact
// daemon route. It reads logical truth only and does no physical publication.
func (a *ChannelActors) AuthorizeAttach(
	id actor.ActorID,
	key actorhost.AttemptKey,
	peer actorhost.ExecutionDomain,
) error {
	if err := a.controller.isCurrent(id, key); err != nil {
		return err
	}
	value, ok, err := a.controller.lookup(id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInactive
	}
	if value.Definition.Placement.Kind != storespec.PlacementDaemon ||
		actorhost.ExecutionDomain(value.Definition.Placement.Host) != peer {
		return ErrInvalidMutation
	}
	return nil
}

// AttachBinding publishes one already-authorized, session-owned exact route.
// A fresh authorization immediately precedes Host publication so a delayed G1
// attach cannot replace G2.
func (a *ChannelActors) AttachBinding(
	id actor.ActorID,
	key actorhost.AttemptKey,
	peer actorhost.ExecutionDomain,
	binding actorhost.Binding,
) error {
	if err := a.AuthorizeAttach(id, key, peer); err != nil {
		return err
	}
	return a.host.Attach(id, key, binding)
}

func (a *ChannelActors) BindingDown(id actor.ActorID, binding actorhost.Binding) {
	if a != nil {
		a.host.BindingDown(id, binding)
	}
}

// Remote lifecycle entry points preserve the same welded A/G admission as a
// direct handle without exporting a handle-minting seam to link.
func (a *ChannelActors) RemoteFork(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	requestID message.ID,
	spec actorcaps.ForkSpec,
) (actor.ActorID, error) {
	result, err := a.Fork(ctx, ForkRequest{
		CallerActorID: id,
		CallerAttempt: key,
		RequestID:     requestID,
		Spec:          spec,
	})
	return result.ChildActorID, err
}

func (a *ChannelActors) RemoteRequestIdle(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
) error {
	return a.requestIdle(ctx, id, key)
}

func (a *ChannelActors) RemoteEndSelf(
	ctx context.Context,
	id actor.ActorID,
	key actorhost.AttemptKey,
	request actorcaps.EndSelfRequest,
) error {
	_, err := a.End(ctx, EndRequest{
		CallerActorID: id,
		CallerAttempt: key,
		Target:        id,
		Reason:        request.Reason,
	})
	return err
}
