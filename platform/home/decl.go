package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrApplyActorEnded      = errors.New("apply_actor_ended")
	ErrApplyVersionNotFound = errors.New("apply_version_not_found")
	ErrApplyVersionRegress  = errors.New("apply_version_regress")
	ErrApplySystemForbidden = errors.New("apply_system_forbidden")
)

func (h *Home) editDeclaration(ctx context.Context, in storespec.DeclEditBundle) (storespec.ActorControlRow, error) {
	if h.closed.Load() {
		return storespec.ActorControlRow{}, ErrClosed
	}
	release := h.actorGates.lock(in.ActorID)
	defer release()
	_, ok, err := h.controlIndex.LookupActive(ctx, in.ActorID)
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	if !ok {
		return storespec.ActorControlRow{}, ErrApplyActorEnded
	}
	world, ok, err := h.controlIndex.WorldOf(ctx, in.ActorID)
	if err != nil || !ok {
		return storespec.ActorControlRow{}, ErrApplyActorEnded
	}
	if world != storespec.WorldDurable {
		return storespec.ActorControlRow{}, ErrApplyVersionNotFound
	}
	return h.cs.DeclVersions.EditDeclared(ctx, in)
}

func (h *Home) applyDeclaration(ctx context.Context, id actor.ActorID, version int64) (storespec.ActorControlRow, error) {
	if h.closed.Load() {
		return storespec.ActorControlRow{}, ErrClosed
	}
	release := h.actorGates.lock(id)
	defer release()
	current, ok, err := h.controlIndex.LookupActive(ctx, id)
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	if !ok {
		return storespec.ActorControlRow{}, ErrApplyActorEnded
	}
	if id == actor.SystemActorID {
		return storespec.ActorControlRow{}, ErrApplySystemForbidden
	}
	world, ok, err := h.controlIndex.WorldOf(ctx, id)
	if err != nil || !ok || world != storespec.WorldDurable {
		return storespec.ActorControlRow{}, ErrApplyVersionNotFound
	}
	if version <= current.CurrentDeclVersion {
		return storespec.ActorControlRow{}, ErrApplyVersionRegress
	}
	if _, found, err := h.cs.Declared.LookupDeclaredVersion(ctx, id, version); err != nil {
		return storespec.ActorControlRow{}, err
	} else if !found {
		return storespec.ActorControlRow{}, ErrApplyVersionNotFound
	}
	row, applied, err := h.cs.DeclVersions.ApplyDeclaredVersion(ctx, id, version)
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	if !applied {
		return storespec.ActorControlRow{}, ErrApplyActorEnded
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		return storespec.ActorControlRow{}, errors.New("platform: invalid applied declaration row")
	}
	h.pokeReconcile()
	return row, nil
}

func (h *Home) declarationVersions(ctx context.Context, id actor.ActorID) (current, latest storespec.ActorControlRow, err error) {
	current, ok, err := h.cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil {
		return current, latest, err
	}
	if !ok {
		return current, latest, ErrApplyActorEnded
	}
	latest, ok, err = h.cs.Declared.LatestDeclaredVersion(ctx, id)
	if err != nil {
		return current, latest, err
	}
	if !ok {
		return current, latest, ErrApplyVersionNotFound
	}
	return current, latest, nil
}
