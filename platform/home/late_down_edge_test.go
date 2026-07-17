package home

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// A late down edge from a replaced predecessor must be rejected by the
// ledger's write-side self-validation (§2.6 组装纪律) — never wipe the
// published successor's carrier/ticket or charge it a spurious restart.
// This drives the exact race the review found: old body evicted from the
// Runtime map (removeIf) → successor rebuilt and published → the old body's
// delayed OnDown lands afterwards. The test replays the tail of that
// sequence deterministically with two REAL incarnations — no production
// hook: it holds the predecessor's incarnation token and delivers its down
// edge after the successor is live.
func TestLateDownEdgeFromReplacedBodyCannotWipeSuccessor(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	id, err := h.Admit(ctx, actor.KindHuman, "late-down-human")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(id)
		return live
	})
	oldInc, _ := h.channel.Cells().CurrentIncarnation(id)

	// Replace the body: despawn the incumbent and let reconcile publish a
	// successor (the same rebuild the reviver/poke path performs mid-window).
	h.channel.Cells().Despawn(oldInc)
	h.pokeReconcile()
	waitHomeCondition(t, func() bool {
		inc, live := h.channel.Cells().CurrentIncarnation(id)
		if !live || inc == oldInc {
			return false
		}
		standing, ok := h.liveness.WakeStanding(id)
		return ok && standing.Occ == occRunning && standing.HasCarrier
	})
	newInc, _ := h.channel.Cells().CurrentIncarnation(id)
	before, _ := h.liveness.WakeStanding(id)
	if before.CarrierInc != newInc {
		t.Fatalf("published carrier token=%v, want successor %v", before.CarrierInc, newInc)
	}
	ticketBefore := h.liveness.AttachmentIntent(id)

	// The predecessor's delayed down edge lands now — stale token, rejected.
	if got := h.liveness.ObserveDown(id, oldInc, false, false); got != transitionInvalid {
		t.Fatalf("late down edge verdict=%v, want invalidTransition (stale token rejected)", got)
	}
	after, ok := h.liveness.WakeStanding(id)
	if !ok || after.Occ != occRunning || !after.HasCarrier || after.Restart || after.CarrierInc != newInc {
		t.Fatalf("successor account damaged by late edge: %+v", after)
	}
	if got := h.liveness.AttachmentIntent(id); got.Ticket != ticketBefore.Ticket {
		t.Fatalf("successor ticket changed by late edge: %q -> %q", ticketBefore.Ticket, got.Ticket)
	}

	// Control: the CURRENT body's own down edge is accepted as ever.
	if got := h.liveness.ObserveDown(id, newInc, false, false); got != transitionApplied {
		t.Fatalf("current down edge verdict=%v, want applied", got)
	}
}
