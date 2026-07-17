package store

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (r *actorRegistry) insertFixedID(ctx context.Context, rec storespec.Record) error {
	principal := rec.Principal
	if principal == "" && rec.Kind != actor.KindSystem {
		principal = string(rec.ID)
	}
	_, err := r.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID: rec.ID, Kind: rec.Kind, Principal: principal, Binding: rec.Binding,
		Class: string(rec.Kind), Placement: storespec.NewServerPlacement(), CreatedAt: rec.CreatedAt,
	})
	return err
}

func (r *actorRegistry) Admit(ctx context.Context, kind actor.Kind, principal string, at int64) (actor.ActorID, error) {
	result, err := r.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: kind, Principal: principal, Class: string(kind), Placement: storespec.NewServerPlacement(), CreatedAt: at,
	})
	return result.ID, err
}

func (r *actorRegistry) Deregister(ctx context.Context, id actor.ActorID, at int64) error {
	_, err := r.EndCascade(ctx, storespec.CascadeBundle{IDs: []actor.ActorID{id}, EndedAt: at})
	return err
}
