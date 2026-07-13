package home

import (
	"context"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func AdmitForTest(h *Home, principal string, kind actor.Kind) (actor.ActorID, error) {
	return h.Admit(context.Background(), kind, principal)
}

func HandleCancelUpstreamForTest(h *Home, id actor.ActorID, requestID message.ID) {
	h.handleCancelUpstream(id, requestID)
}

// DriveDownEdgeForTest posts id onto the REAL death-edge path (OnDown → downCh →
// consumeDown → closeFor), exactly as the runtime does when a cell dies. Used by
// the C7 注销+边沿竞态 test to race a late death edge against the level scan on the
// same closed-forever fact; the incarnation is ignored by OnDown (a corpse edge).
func DriveDownEdgeForTest(h *Home, id actor.ActorID) {
	h.channel.OnDown(context.Background(), id, actorrt.Incarnation{}, nil)
}

// ReconcileClosureForTest synchronously drives the closure LEVEL scan
// (channel.Reconcile) — the second author path — so a test can interleave it with
// DriveDownEdgeForTest without depending on the background ticker's cadence.
func ReconcileClosureForTest(h *Home) {
	h.channel.Reconcile(context.Background())
}

func SpawnForTest(h *Home, fixtureID actor.ActorID, kind actor.Kind, def platform.ActorFactory) (actor.ActorID, error) {
	id := fixtureID
	if rec, ok, err := h.cs.Registry.Lookup(context.Background(), fixtureID); err != nil {
		return "", err
	} else if !ok || !rec.IsActive() {
		id, err = AdmitForTest(h, strings.ReplaceAll(string(fixtureID), ":", "-"), kind)
		if err != nil {
			return "", err
		}
	}
	_, built, err := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, kind, inc), h.hooks(), def)
	})
	if err != nil {
		return "", err
	}
	if !built {
		return "", fmt.Errorf("test spawn %q already occupied", id)
	}
	return id, nil
}
