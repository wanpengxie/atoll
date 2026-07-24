package home

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// admitChannelOwner is the bootstrap-only owner admission. It still enters
// Controller, so the committed identity and desired execution are published
// through the same Controller path as every other managed actor.
func (h *Home) admitChannelOwner(ctx context.Context, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	result, err := h.actors.Admit(ctx, actorctl.AdmitRequest{
		Principal: principal,
		Role:      storespec.RoleOwner,
	})
	if err != nil {
		return "", fmt.Errorf("platform: admit channel owner: %w", err)
	}
	h.ensureSubjectSlot(result.ActorID)
	return result.ActorID, nil
}
