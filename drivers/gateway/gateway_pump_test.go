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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestStaticBacklogFullDelivery (DoD-7⑤): a channel with a static backlog >2×feedBatch
// and zero new writes is fully delivered — the pump's 积压续跑 keeps a channel runnable
// while it reads a full batch, so it drains the whole tail rather than one batch per
// edge. Asserted on the real downstream feed reaching the channel head.
func TestStaticBacklogFullDelivery(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "rob"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 2*feedBatch+5) // >2×feedBatch rows beyond the member's own admit row
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	if head <= 2*feedBatch {
		t.Fatalf("test needs a backlog >2×feedBatch, got head=%d", head)
	}

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()

	waitFor(t, func() bool { return len(feed.sequences("c")) >= len(want) },
		"static backlog was not fully delivered (积压续跑 broken)")
	s.Close()
	stop()
	if got := feed.sequences("c"); !slices.Equal(got, want) {
		t.Fatalf("downstream feed sequence mismatch: got %v want %v", got, want)
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
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
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
	hotHead, _ := hot.View().MaxSeq(context.Background())
	s, _ := g.Attach(principal, nil)
	var feed *feedObserver
	var stop func()
	coldReached := make(chan int64, 1)
	var coldOnce sync.Once
	feed, stop = observeFeed(s, func(ch channel.ID, _ int) {
		if ch == "cold" && feed.lastSeq("cold") >= coldHead {
			coldOnce.Do(func() { coldReached <- int64(feed.delivered("hot")) })
		}
	})
	s.StartFeed()

	var hotAtCold int64
	select {
	case hotAtCold = <-coldReached:
	case <-time.After(2 * time.Second):
		t.Fatal("cold channel starved by the hot channel (fairness broken)")
	}
	// At the exact wire boundary where cold reaches its head, hot may have emitted at
	// most its one batch for that round. An implementation that drains hot completely
	// before visiting cold therefore fails instead of passing on eventual delivery.
	if hotAtCold > feedBatch || hotAtCold >= int64(len(sourceSeqs(t, hot))) {
		t.Fatalf("cold arrived only after hot overran its one-batch turn: hot=%d head=%d batch=%d", hotAtCold, hotHead, feedBatch)
	}
	s.Close()
	stop()
}

func sourceSeqs(t *testing.T, h *testChannel) []int64 {
	t.Helper()
	var seqs []int64
	after := int64(0)
	for {
		rows, scanned, err := h.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{
			ActorID: h.memberID, Mode: channel.ReaderMember,
		}, after, feedBatch)
		if err != nil {
			t.Fatalf("ReadAfterSeq(%d): %v", after, err)
		}
		for _, row := range rows {
			seqs = append(seqs, row.Seq)
		}
		after = scanned
		if len(rows) < feedBatch {
			break
		}
	}
	if len(seqs) == 0 {
		t.Fatal("source log unexpectedly empty")
	}
	return seqs
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
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk, sweepInterval: 10 * time.Millisecond})
	const principal = "revoked-busy"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3*feedBatch) // deep backlog: every batch read is a full feedBatch.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	head, err := h.View().MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	revoked := make(chan struct{})
	var revokeOnce sync.Once
	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s, func(ch channel.ID, count int) {
		if ch != "c" || count != 1 {
			return
		}
		revokeOnce.Do(func() {
			// Revoke while the first full batch is visibly crossing the downstream
			// boundary. runFeed has armed its sweep before it can emit this frame.
			res.set(principal, nil, nil, nil)
			clk.advance(10 * time.Millisecond)
			close(revoked)
		})
	})
	s.StartFeed()

	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("pump never began crossing the first full batch")
	}

	waitFor(t, func() bool {
		_, ok := eligRoutes(s)["c"]
		return !ok
	}, "revoked channel must retire within a bounded time even under a sustained busy backlog (P0-1: busy→continue must still observe sweep/poke)")

	// The backlog must still be far from fully drained at the moment of retirement —
	// otherwise this test would pass even on the pre-fix code merely because the busy
	// loop happened to finish the whole backlog before anyone looked (proving nothing
	// about bounded revocation response).
	stoppedAt := feed.lastSeq("c")
	if stoppedAt >= head {
		t.Fatalf("backlog (%d rows) fully drained before the sweep-bound revocation could be observed; deepen the backlog or shrink SweepInterval — stoppedAt=%d head=%d", 3*feedBatch, stoppedAt, head)
	}

	s.Close()
	stop()
}

// TestAdmitWithoutPokeConvergesOnSweep (DoD-6/7③ timer backstop): the connection is
// already running with no channels. A real Home.Admit then appears in resolver truth,
// but there is deliberately NO OnMembershipChange wire and no hand Poke. Advancing the
// injected clock fires runFeed's real sweep timer and the new channel enters the stream.
func TestAdmitWithoutPokeConvergesOnSweep(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const sweep = 10 * time.Second
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk, sweepInterval: sweep})
	const principal = "no-poke-admit"
	res.set(principal, nil, nil, nil)

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()
	waitFor(t, func() bool { return res.callCount() >= 2 && clk.armCount() >= 2 },
		"running pump did not arm its sweep timer")

	// Admit after the pump is waiting. openHome has no membership callback, so this
	// truth change cannot reach the gateway except through the periodic sweep.
	h, id := openHome(t, channel.ID("late"), principal)
	admitRows(t, h, 1)
	res.set(principal, []Route{memberRoute("late", h, id, clk.now())}, nil, nil)
	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	clk.advance(sweep)
	waitFor(t, func() bool { return feed.lastSeq("late") >= head },
		"no-poke Admit did not converge through the real sweep timer")

	s.Close()
	stop()
}

// TestAdmitPokeEntersStream (DoD-7③): a running pump with NO eligibility yet; when the
// resolver gains a channel and a poke fires (the Admit membership-change poke), the
// channel enters the stream within a bounded time (≤下一泵轮) — the dirty/wake edge
// re-resolves, subscribes, and pumps the backlog. Asserted on the real feed advancing.
func TestAdmitPokeEntersStream(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "tom"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 1)
	// Start with NO eligibility for tom.
	res.set(principal, nil, nil, nil)

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()
	// Initially no subscription (nothing eligible).
	waitFor(t, func() bool { return len(eligRoutes(s)) == 0 }, "expected no eligibility initially")

	// Admit lands: eligibility appears + a poke踹 the pump.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	g.Poke(principal)

	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	waitFor(t, func() bool { return feed.lastSeq("c") >= head },
		"Admit poke did not bring the channel into the stream (≤下一泵轮 broken)")
	s.Close()
	stop()
}
