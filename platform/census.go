package platform

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Admit registers one actor as durable channel membership truth and nothing more
// — the pure-membership primitive (the not→member edge of §4.6). It writes a
// NEUTRAL row (Binding="" / Host=""): membership precedes embodiment, and the
// host path (daemon attach / activation ring) owns Binding/Host stamping — Admit
// never guesses placement. It does not Mint a pen or place a cell; the desired
// member is embodied by the reconcile ring's SpawnIfAbsent (activation) or a
// daemon attach, never by Admit itself. After the write it pokes the ring so the
// embodiment lands on the next immediate sweep rather than waiting a full tick.
// Idempotent for an already-active (kind, principal): the registry returns the
// existing minted instance id and the extra reconcile poke is harmless.
func (h *Home) Admit(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	id, err := h.cs.Membership.Admit(ctx, kind, principal, h.nowMs())
	if err != nil {
		return "", fmt.Errorf("platform: Admit membership: %w", err)
	}
	// 装配链 step② (gateway 期 v0.4.1 勘误): a human's binding slot生死随户籍级联 — ensure
	// it at准入 (before the reconcile poke, so it strictly precedes any gateway attach
	// that could look it up), synchronously so a client that attaches right after Admit
	// never races an absent slot. Idempotent with factoryFor's ensure (restart path).
	if kind == actor.KindHuman {
		h.EnsureSubjectSlot(id)
	}
	h.pokeReconcile()
	h.logger.Info("platform.member.admitted", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind), "principal", principal)
	return id, nil
}

// PrincipalOf returns the opaque principal recorded for an actor instance.
func (h *Home) PrincipalOf(ctx context.Context, id actor.ActorID) (string, bool, error) {
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		return "", ok, err
	}
	return rec.Principal, true, nil
}

func (h *Home) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	reg, ok := h.cs.Registry.(storespec.PrincipalRegistry)
	if !ok {
		return "", false, errors.New("platform: principal registry unavailable")
	}
	rec, found, err := reg.LookupActivePrincipal(ctx, kind, principal)
	return rec.ID, found, err
}
