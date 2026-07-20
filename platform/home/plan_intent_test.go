package home

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestDaemonPlanProjectsLivenessIntentAndStableEnsureTicket(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "plan-parent")
	if err != nil {
		t.Fatal(err)
	}
	placement, _ := storespec.NewDaemonPlacement("daemon-a")
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "worker", Placement: &placement,
	}, "plan-child")
	if err != nil {
		t.Fatal(err)
	}
	if plan, err := h.planForDaemon(ctx, "daemon-a"); err != nil || len(plan) != 0 {
		t.Fatalf("dormant plan=%v err=%v", plan, err)
	}
	_, _ = h.liveness.AcceptDelivery(child, &message.Envelope{Kind: message.KindRequest})
	h.reconcileDaemonIntent(ctx)
	plan, err := h.planForDaemon(ctx, "daemon-a")
	if err != nil || len(plan) != 1 || plan[0].InstanceID != child || plan[0].EnsureTicket == "" || plan[0].TIdleMs != defaultForkIdle.Milliseconds() {
		t.Fatalf("dirty plan=%+v err=%v", plan, err)
	}
	ticket := plan[0].EnsureTicket
	again, _ := h.planForDaemon(ctx, "daemon-a")
	if len(again) != 1 || again[0].EnsureTicket != ticket {
		t.Fatalf("repeated plan changed ticket: first=%+v again=%+v", plan, again)
	}

	q := &testCarrier{}
	if got := h.liveness.Attach(child, EnsureTicket(ticket), 1, noInc, q); got != transitionApplied {
		t.Fatalf("attach=%v", got)
	}
	if _, verdict := h.liveness.ApproveIdle(child); verdict != transitionApplied {
		t.Fatalf("idle=%v", verdict)
	}
	if plan, _ := h.planForDaemon(ctx, "daemon-a"); len(plan) != 0 {
		t.Fatalf("idle actor remains in plan: %+v", plan)
	}

	// A later request creates a new attempt; a port loss preserves that ticket
	// in detached state so the same-version rebind is accepted.
	_, _ = h.liveness.AcceptDelivery(child, &message.Envelope{Kind: message.KindRequest})
	h.reconcileDaemonIntent(ctx)
	rebuilt, _ := h.planForDaemon(ctx, "daemon-a")
	if len(rebuilt) != 1 || rebuilt[0].EnsureTicket == ticket {
		t.Fatalf("new attempt=%+v oldTicket=%q", rebuilt, ticket)
	}
	newTicket := EnsureTicket(rebuilt[0].EnsureTicket)
	if h.liveness.Attach(child, newTicket, 1, noInc, q) != transitionApplied {
		t.Fatal("new attempt attach")
	}
	if h.liveness.ObserveDown(child, noInc, true, false) != transitionApplied {
		t.Fatal("port down did not detach")
	}
	detached, _ := h.planForDaemon(ctx, "daemon-a")
	if len(detached) != 1 || detached[0].EnsureTicket != string(newTicket) {
		t.Fatalf("detached plan=%+v", detached)
	}
	if h.liveness.Attach(child, newTicket, 1, noInc, q) != transitionApplied {
		t.Fatal("same-ticket rebind rejected")
	}

	// Manual restart of a present daemon carrier retires it and produces a fresh
	// ticket at the same declaration version. Dormant restart above remained a
	// no-op by construction.
	if _, err := h.restartInstanceDirect(ctx, child); err != nil {
		t.Fatal(err)
	}
	h.reconcileDaemonIntent(ctx)
	restarted, _ := h.planForDaemon(ctx, "daemon-a")
	if len(restarted) != 1 || restarted[0].Version != 1 || restarted[0].EnsureTicket == string(newTicket) {
		t.Fatalf("manual restart plan=%+v", restarted)
	}

	// Let any coalesced background reconcile finish before Home cleanup; this is
	// not a correctness wait, only keeps race output focused on this test.
	time.Sleep(time.Millisecond)
}
