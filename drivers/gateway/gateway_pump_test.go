package gateway

// Read pump tests (spec §3.2 收敛对象甲 pump phase): static backlog full delivery
// (DoD-7⑤), round-robin fairness (DoD-9), and Admit poke → ≤下一泵轮入流 (DoD-7③).
// These exercise the REAL runFeed goroutine over a REAL Home; the pump wakes on
// poke/Home-signal immediately, so the 30s backstop timers never fire in-test.

import (
	"context"
	"testing"

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
