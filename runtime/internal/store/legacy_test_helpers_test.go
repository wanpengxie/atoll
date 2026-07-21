package store

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (r *actorRegistry) insertFixedID(ctx context.Context, rec storespec.Record) error {
	principal := rec.Principal
	source := ""
	if rec.Kind == actor.KindAgent || rec.Kind == actor.KindTool {
		source = "test:" + string(rec.ID)
	}
	_, err := r.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID: rec.ID, Kind: rec.Kind, Principal: principal, Binding: rec.Binding,
		Class: string(rec.Kind), SourceDeclID: source, Placement: storespec.NewServerPlacement(), CreatedAt: rec.CreatedAt,
	})
	return err
}

func endActorForTest(ctx context.Context, r *actorRegistry, id actor.ActorID, at int64) error {
	_, err := r.EndCascade(ctx, storespec.CascadeBundle{IDs: []actor.ActorID{id}, EndedAt: at, Envelopes: []storespec.CascadeEnvelope{{Target: id, EndedBy: actor.SystemActorID}}})
	return err
}
