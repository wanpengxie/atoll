package gateway

// Read pump tests (spec §3.2 收敛对象甲 pump phase): static backlog full delivery
// (DoD-7⑤), round-robin fairness (DoD-9), Admit poke → ≤下一泵轮入流 (DoD-7③), and
// busy-loop sweep observation under sustained backlog with NO poke (P0-1, 六轮终审).
// Most of these exercise the REAL runFeed goroutine over a REAL Home, waking on
// poke/Home-signal immediately so the default 30s backstop timers never fire in-test;
// TestBusyLoopObservesSweepUnderSustainedBacklog is the one exception — it injects a
// short SweepInterval via Config specifically to drive convergence off the timer
// backstop alone (no poke at all), the scenario this file previously had zero coverage
// for.

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestStaticBacklogFullDelivery (DoD-7⑤): a channel with a static backlog >2×feedBatch
// and zero new writes is fully delivered — the pump's 积压续跑 keeps a channel runnable
// while it reads a full batch, so it drains the whole tail rather than one batch per
// edge. Asserted on the lane cursor reaching the channel head (every row pushed).
func TestStaticBacklogFullDelivery(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk.now})
	const principal = "rob"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 2*feedBatch+5) // >2×feedBatch rows beyond the member's own admit row
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	head, err := h.View().MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if head <= 2*feedBatch {
		t.Fatalf("test needs a backlog >2×feedBatch, got head=%d", head)
	}

	s, _ := g.Attach(context.Background(), principal, nil)
	_, stop := drainFeed(s)
	s.StartFeed()

	waitFor(t, func() bool { return s.lane.cursor.at("c") >= head },
		"static backlog was not fully delivered (积压续跑 broken)")
	s.Close()
	stop()
	if got := s.lane.cursor.at("c"); got != head {
		t.Fatalf("cursor must reach the channel head: got %d want %d", got, head)
	}
}

// TestPumpFairness (DoD-9): a hot channel with a large backlog must not starve a cold
// channel with a tiny one — round-robin one batch per channel per轮 delivers the cold
// channel's rows within a couple of rotations, long before the hot channel finishes
// draining. Asserted: the cold channel reaches its head while the hot channel may still
// be draining.
func TestPumpFairness(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk.now})
	const principal = "sam"
	hot, hotID := openHome(t, channel.ID("hot"), principal)
	cold, coldID := openHome(t, channel.ID("cold"), principal)
	admitRows(t, hot, 2*feedBatch+5) // multi-batch backlog (round-robin one batch/轮)
	admitRows(t, cold, 3)            // tiny backlog

	res.set(principal, []Route{
		memberRoute("hot", hot, hotID, clk.now()),
		memberRoute("cold", cold, coldID, clk.now()),
	}, nil, nil)

	coldHead, _ := cold.View().MaxSeq(context.Background())
	s, _ := g.Attach(context.Background(), principal, nil)
	_, stop := drainFeed(s)
	s.StartFeed()

	// The cold channel reaches its head promptly (not starved by the hot backlog).
	waitFor(t, func() bool { return s.lane.cursor.at("cold") >= coldHead },
		"cold channel starved by the hot channel (fairness broken)")
	s.Close()
	stop()
}

// TestBusyLoopObservesSweepUnderSustainedBacklog (P0-1 终审锚): a channel with a deep
// sustained backlog (every batch read is a FULL feedBatch) keeps the pump on the
// busy→continue path indefinitely — before the fix, that path never called wait(), so
// the fired periodic sweep timer was never drained and dirty never got re-armed. A
// caller who revokes eligibility with NO poke (the exact "poke lost/never sent"
// scenario codex's terminal review named) would then stream past the revocation with NO
// upper bound — a permission data leak, not an in-一圈 advisory偏差. This test injects a
// short SweepInterval and asserts the channel retires within a bounded real-time window
// WHILE the backlog is still far from fully drained (proving the busy loop, not merely
// the eventual drain-to-completion, is what caught the revocation).
func TestBusyLoopObservesSweepUnderSustainedBacklog(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk.now, SweepInterval: 10 * time.Millisecond})
	const principal = "revoked-busy"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3*feedBatch) // deep backlog: every batch read is a full feedBatch.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	s, _ := g.Attach(context.Background(), principal, nil)
	_, stop := drainFeed(s)
	s.StartFeed()

	// Let the pump get busy (at least one full batch already read) before revoking.
	waitFor(t, func() bool { return s.lane.cursor.at("c") >= feedBatch },
		"pump never started draining the backlog")

	head, err := h.View().MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}

	// Revoke NOW, with NO poke — the read side must self-discover this via the sweep
	// backstop even while continuously busy (P0-1).
	res.set(principal, nil, nil, nil)

	waitFor(t, func() bool {
		_, ok := eligRoutes(s)["c"]
		return !ok
	}, "revoked channel must retire within a bounded time even under a sustained busy backlog (P0-1: busy→continue must still observe sweep/poke)")

	// The backlog must still be far from fully drained at the moment of retirement —
	// otherwise this test would pass even on the pre-fix code merely because the busy
	// loop happened to finish the whole backlog before anyone looked (proving nothing
	// about bounded revocation response).
	stoppedAt := s.lane.cursor.at("c")
	if stoppedAt >= head {
		t.Fatalf("backlog (%d rows) fully drained before the sweep-bound revocation could be observed; deepen the backlog or shrink SweepInterval — stoppedAt=%d head=%d", 3*feedBatch, stoppedAt, head)
	}

	s.Close()
	stop()
}

// TestAdmitPokeEntersStream (DoD-7③): a running pump with NO eligibility yet; when the
// resolver gains a channel and a poke fires (the Admit membership-change poke), the
// channel enters the stream within a bounded time (≤下一泵轮) — the dirty/wake edge
// re-resolves, subscribes, and pumps the backlog. Asserted on the cursor advancing.
func TestAdmitPokeEntersStream(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk.now})
	const principal = "tom"
	h, id := openHome(t, channel.ID("c"), principal)
	// Start with NO eligibility for tom.
	res.set(principal, nil, nil, nil)

	s, _ := g.Attach(context.Background(), principal, nil)
	_, stop := drainFeed(s)
	s.StartFeed()
	// Initially no subscription (nothing eligible).
	waitFor(t, func() bool { return len(eligRoutes(s)) == 0 }, "expected no eligibility initially")

	// Admit lands: eligibility appears + a poke踹 the pump.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	g.Poke(principal)

	head, _ := h.View().MaxSeq(context.Background())
	waitFor(t, func() bool { return s.lane.cursor.at("c") >= head },
		"Admit poke did not bring the channel into the stream (≤下一泵轮 broken)")
	s.Close()
	stop()
}
