package home

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var ErrAdmitKind = errors.New("platform: membership admission accepts only human actors")

// Admit creates one durable human identity through the declared-admission
// transaction. It does not Mint a pen or place a cell; the control row declares
// server placement and the reconcile ring owns embodiment. After publication it
// pokes the ring so the
// embodiment lands on the next immediate sweep rather than waiting a full tick.
// Idempotent for an already-active (kind, principal): the registry returns the
// existing minted instance id and the extra reconcile poke is harmless.
func (h *Home) admit(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	if kind != actor.KindHuman {
		return "", ErrAdmitKind
	}
	return h.admitHuman(ctx, storespec.AdmitBundle{
		Kind: actor.KindHuman, Principal: principal, Class: "human",
		Placement: storespec.NewServerPlacement(), CreatedAt: h.nowMs(),
	})
}

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
	row, ok, err := h.cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil || !ok {
		if err == nil {
			err = errors.New("committed actor missing from declared read face")
		}
		return "", fmt.Errorf("platform: publish admitted actor %s: %w", id, err)
	}
	if in.Role == storespec.RoleOwner && row.Role != storespec.RoleOwner {
		return "", fmt.Errorf("platform: admitted actor %s role mismatch: got %q want %q", id, row.Role, in.Role)
	}
	// Assembly order (装配序): liveness row BEFORE authority publish — same
	// rule as fork/declared admission (a delivery racing the publish must
	// find the L row and record wake debt, never be silently skipped).
	if h.liveness.AdmitIdentity(id) != transitionApplied {
		return "", fmt.Errorf("platform: publish admitted actor %s: liveness rejected", id)
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		return "", fmt.Errorf("platform: publish admitted actor %s: invalid control row", id)
	}
	// 装配链 step② (gateway 期 v0.4.1 勘误): a human's slot (在场与递交接头盒)生死随户籍级联 — ensure
	// it at准入 (before the reconcile poke, so it strictly precedes any gateway attach
	// that could look it up), synchronously so a client that attaches right after Admit
	// never races an absent slot. Idempotent with factoryFor's ensure (restart path).
	h.ensureSubjectSlot(id)
	h.pokeReconcile()
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

// PrincipalOf returns the opaque principal recorded for an actor instance.
func (h *Home) principalOf(ctx context.Context, id actor.ActorID) (string, bool, error) {
	if h.closed.Load() {
		return "", false, ErrClosed
	}
	rec, ok, err := h.cs.Authority.LookupActive(ctx, id)
	if err != nil || !ok {
		return "", ok, err
	}
	return rec.Principal, true, nil
}

func (h *Home) resolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	// Principals is its own assembly-declared face (ChannelStores.Principals);
	// the old type-assertion that recovered it from the narrow Registry field
	// was a bypass valve (it voided the read-face segregation for every
	// ChannelStores holder at once) and is gone — purity 手动档, 反旁路结构墙.
	rec, found, err := h.cs.Principals.LookupActivePrincipal(ctx, kind, principal)
	return rec.ID, found, err
}
