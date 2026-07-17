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
	result, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: kind, Principal: principal, Class: string(kind),
		Placement: storespec.NewServerPlacement(), CreatedAt: at,
	})
	return result.ID, err
}

func endDeclaredTest(ctx context.Context, cs *ChannelStores, id actor.ActorID, at int64) error {
	_, err := cs.Cascade.EndCascade(ctx, storespec.CascadeBundle{IDs: []actor.ActorID{id}, EndedAt: at})
	return err
}

func scheduleStamp(id actor.ActorID) storespec.AuthorStamp {
	return storespec.AuthorStamp{ID: id, BirthVersion: 1}
}
