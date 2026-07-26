package runtime

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func admitDeclaredTest(ctx context.Context, cs *ChannelStores, kind actor.Kind, principal string, at int64) (actor.ActorID, error) {
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	draft := storespec.ActorDraft{
		Kind:       kind,
		Definition: storespec.ActorDefinition{Class: string(kind)},
		Placement:  storespec.NewServerPlacement(), CreatedAt: at,
	}
	if kind == actor.KindHuman {
		draft.Principal = principal
	} else if kind == actor.KindAgent || kind == actor.KindTool {
		draft.SourceDeclID = principal
	}
	record, err := cs.Actors.Insert(ctx, draft)
	return record.ID, err
}

func endDeclaredTest(ctx context.Context, cs *ChannelStores, id actor.ActorID, at int64) error {
	return cs.Actors.Deregister(ctx, []actor.ActorID{id}, at)
}

func identityAdmission(id actor.ActorID) storespec.IdentityAdmission {
	return storespec.IdentityAdmission{ID: id, Kind: actor.KindAgent}
}
