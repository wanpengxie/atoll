package home

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
)

// admitChannelOwner is the bootstrap-only owner admission. It is an ORDINARY
// human admission: owner-ness is not seeded at the door, it is derived from the
// genesis pointer wherever the question is asked.
func (h *Home) admitChannelOwner(ctx context.Context, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	result, err := h.actors.Admit(ctx, actorctl.AdmitRequest{Principal: principal})
	if err != nil {
		return "", fmt.Errorf("platform: admit channel owner: %w", err)
	}
	h.ensureSubjectSlot(result.ActorID)
	h.narrateBirth(ctx, result.ActorID, actor.KindHuman, result.Created)
	return result.ActorID, nil
}
