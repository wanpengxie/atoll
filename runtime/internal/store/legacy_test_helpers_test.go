package store

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (r *actorRegistry) insertFixedID(ctx context.Context, rec storespec.Record) error {
	source := ""
	if rec.Kind == actor.KindAgent || rec.Kind == actor.KindTool {
		source = "test:" + string(rec.ID)
	}
	_, err := r.Insert(ctx, storespec.ActorDraft{
		ID: rec.ID, Kind: rec.Kind, Principal: rec.Principal,
		SourceDeclID: source, CreatedAt: rec.CreatedAt,
		Definition: storespec.ActorDefinition{Class: string(rec.Kind)},
		Placement:  storespec.NewServerPlacement(),
	})
	return err
}

func endActorForTest(ctx context.Context, r *actorRegistry, id actor.ActorID, at int64) error {
	return r.Deregister(ctx, []actor.ActorID{id}, at)
}
