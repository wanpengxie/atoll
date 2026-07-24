package actorctl

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// LookupActive exposes Controller's coherent identity projection. The system
// identity is composed from SystemKernel state rather than copied into the
// managed map.
func (a *ChannelActors) LookupActive(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorControlRow, bool, error) {
	return a.Lookup(id)
}

func (a *ChannelActors) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return a.listActiveRows()
}

func (a *ChannelActors) WorldOf(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorWorld, bool, error) {
	if id == actor.SystemActorID {
		if _, ok := a.Stat(id); !ok && a.controller.phaseValue() != Running {
			return 0, false, ErrClosed
		}
		return storespec.WorldDurable, true, nil
	}
	value, ok, err := a.controller.lookup(id)
	if err != nil || !ok {
		return 0, ok, err
	}
	if value.Definition.Origin == OriginRunWorld {
		return storespec.WorldRun, true, nil
	}
	return storespec.WorldDurable, true, nil
}

// CheckAuthor intentionally ignores DefinitionVersion. It is collaboration
// authority (active ActorID), not an incarnation/config-generation fence.
func (a *ChannelActors) CheckAuthor(
	ctx context.Context,
	stamp storespec.AuthorStamp,
) (storespec.AuthorVerdict, error) {
	_, active, err := a.LookupActive(ctx, stamp.ID)
	if err != nil {
		return storespec.AuthorNotMember, err
	}
	if !active {
		return storespec.AuthorNotMember, nil
	}
	return storespec.AuthorOK, nil
}

var _ storespec.ActorAuthority = (*ChannelActors)(nil)
