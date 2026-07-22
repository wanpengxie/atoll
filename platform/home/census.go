package home

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// AdmitChannelOwner performs the sole non-neutral durable admission. A
// principal collision converges in the store, so the post-commit read must
// prove the existing row is already the owner; admission never upgrades an
// ordinary member in place.
func (h *Home) admitChannelOwner(ctx context.Context, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	return h.admitHuman(ctx, storespec.AdmitBundle{
		Kind: actor.KindHuman, Principal: principal, Class: "human",
		Role:      storespec.RoleOwner,
		Placement: storespec.NewServerPlacement(), CreatedAt: h.nowMs(),
	})
}

func (h *Home) admitHuman(ctx context.Context, in storespec.AdmitBundle) (actor.ActorID, error) {
	result, err := h.cs.DeclAdmission.AdmitDeclared(ctx, in)
	if err != nil {
		return "", fmt.Errorf("platform: Admit declared actor: %w", err)
	}
	id := result.ID
	if _, err := h.publishDeclaredActor(ctx, id, in.Role); err != nil {
		return "", fmt.Errorf("platform: publish admitted actor %s: %w", id, err)
	}
	// Membership-change poke emit point (连接模型勘误期 §3.2 表②, Admit 侧新增): the
	// person gained a channel — poke so their gateway session re-resolves its
	// subscriptions/presence on the next immediate loop rather than waiting a sweep.
	// Pure及时性 (a lost poke only delays convergence); the principal is right here.
	if h.onMembershipChange != nil {
		h.onMembershipChange(in.Principal)
	}
	if result.Created {
		h.logger.Info("platform.member.admitted", "channel", string(h.channelID),
			"actor", string(id), "kind", string(actor.KindHuman), "principal", in.Principal, "role", string(in.Role))
	}
	return id, nil
}
