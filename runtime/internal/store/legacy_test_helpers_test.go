package store

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// insertTool admits one tool actor and returns the id the registry minted for
// it. Tests name the declaration, never the actor: a birth id is minted inside
// the insert transaction and there is no way to ask for a particular one.
func (r *actorRegistry) insertTool(ctx context.Context, decl string) (actor.ActorID, error) {
	record, err := r.Insert(ctx, storespec.ActorDraft{
		Kind: actor.KindTool, SourceDeclID: "test:" + decl, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: string(actor.KindTool)},
		Placement:  storespec.NewServerPlacement(),
	})
	return record.ID, err
}

func endActorForTest(ctx context.Context, r *actorRegistry, id actor.ActorID, at int64) error {
	return r.Deregister(ctx, []actor.ActorID{id}, at)
}
