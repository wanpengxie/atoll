package home

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var ErrAdmitKind = errors.New("platform: Home.Admit accepts only human actors")

// Admit creates one durable human identity through the declared-admission
// transaction. It does not Mint a pen or place a cell; the control row declares
// server placement and the reconcile ring owns embodiment. After publication it
// pokes the ring so the
// embodiment lands on the next immediate sweep rather than waiting a full tick.
// Idempotent for an already-active (kind, principal): the registry returns the
// existing minted instance id and the extra reconcile poke is harmless.
func (h *Home) Admit(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	if kind != actor.KindHuman {
		return "", ErrAdmitKind
	}
	result, err := h.cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: actor.KindHuman, Principal: principal, Class: "human",
		Placement: storespec.NewServerPlacement(), CreatedAt: h.nowMs(),
	})
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
	h.EnsureSubjectSlot(id)
	h.pokeReconcile()
	// Membership-change poke emit point (连接模型勘误期 §3.2 表②, Admit 侧新增): the
	// person gained a channel — poke so their gateway session re-resolves its
	// subscriptions/presence on the next immediate loop rather than waiting a sweep.
	// Pure及时性 (a lost poke only delays convergence); the principal is right here.
	if h.onMembershipChange != nil {
		h.onMembershipChange(principal)
	}
	if result.Created {
		h.logger.Info("platform.member.admitted", "channel", string(h.channelID),
			"actor", string(id), "kind", string(kind), "principal", principal)
	}
	return id, nil
}

// PrincipalOf returns the opaque principal recorded for an actor instance.
func (h *Home) PrincipalOf(ctx context.Context, id actor.ActorID) (string, bool, error) {
	if h.closed.Load() {
		return "", false, ErrClosed
	}
	rec, ok, err := h.cs.Authority.LookupActive(ctx, id)
	if err != nil || !ok {
		return "", ok, err
	}
	return rec.Principal, true, nil
}

func (h *Home) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	// Principals is its own assembly-declared face (ChannelStores.Principals);
	// the old type-assertion that recovered it from the narrow Registry field
	// was a bypass valve (it voided the read-face segregation for every
	// ChannelStores holder at once) and is gone — purity 手动档, 反旁路结构墙.
	rec, found, err := h.cs.Principals.LookupActivePrincipal(ctx, kind, principal)
	return rec.ID, found, err
}
