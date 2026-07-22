package home

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// publishDeclaredActor installs a committed durable identity into the run-world
// indexes. The order is an assembly invariant: liveness must exist before
// authority becomes visible, otherwise a racing delivery can advance without
// recording wake debt.
func (h *Home) publishDeclaredActor(ctx context.Context, id actor.ActorID, expectedRole storespec.ActorRole) (storespec.ActorControlRow, error) {
	row, found, err := h.cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil || !found {
		if err == nil {
			err = errors.New("committed actor missing from declared view")
		}
		return storespec.ActorControlRow{}, err
	}
	if expectedRole != storespec.RoleNone && row.Role != expectedRole {
		return storespec.ActorControlRow{}, fmt.Errorf("actor %s role mismatch: got %q want %q", id, row.Role, expectedRole)
	}
	if h.liveness.AdmitIdentity(id) != transitionApplied {
		return storespec.ActorControlRow{}, fmt.Errorf("actor %s liveness rejected", id)
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		return storespec.ActorControlRow{}, fmt.Errorf("actor %s control index rejected", id)
	}
	if row.Kind == actor.KindHuman {
		h.ensureSubjectSlot(id)
	}
	h.pokeReconcile()
	return row, nil
}
