package gateway

// 资格对账 (spec §3.2 收敛对象甲) + T_stale 租约 (DoD-8) tests: subscribe/retire,
// whole-snapshot-failure lease → pause → resume, per-channel 部分失败 vs confirmed-absent,
// and StartFeed's synchronous first reconcile (connect-then-act).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// eligRoutes reads the session's published资格账 route set (test helper).
func eligRoutes(s *Session) map[channel.ID]Route    { return s.elig.Load().routes }
func eligPaused(s *Session) map[channel.ID]struct{} { return s.elig.Load().paused }

// TestReconcileSubscribeThenRetire (成功查得无资格立即退订): a member route → reconcile
// subscribes + publishes it in the资格账; when the resolver later confirms NO
// eligibility (absent from BOTH routes and failed), the next reconcile退订 immediately —
// no lease for a confirmed absence.
func TestReconcileSubscribeThenRetire(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk})
	const principal = "mia"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)
	defer s.Close()

	s.reconcile()
	if _, ok := eligRoutes(s)["c"]; !ok {
		t.Fatal("a member route must be subscribed + published after reconcile")
	}
	if len(s.subs) != 1 {
		t.Fatalf("expected one subscription, got %d", len(s.subs))
	}

	// Confirmed no eligibility → immediate退订 (not a lease).
	res.set(principal, nil, nil, nil)
	s.reconcile()
	if _, ok := eligRoutes(s)["c"]; ok {
		t.Fatal("confirmed-absent channel must退订 immediately")
	}
	if len(s.subs) != 0 {
		t.Fatalf("subscription must be dropped on confirmed absence, got %d", len(s.subs))
	}
	if _, paused := eligPaused(s)["c"]; paused {
		t.Fatal("a confirmed absence must NOT be treated as a paused lease")
	}
}

// TestLeaseThenPauseThenResume (DoD-8, whole-snapshot failure): after a successful
// check, a resolver error rides the T_stale lease (still served within 30s of the last
// SUCCESS), then pauses the channel past the lease; a later success resumes it. Clock is
// injected — no real 30s wait.
func TestLeaseThenPauseThenResume(t *testing.T) {
	clk := newClock()
	res := newResolver()
	cap := &logCapture{}
	g := New(Config{Resolver: res, Clock: clk, Logger: cap.logger()})
	const principal = "nan"
	h, id := openHome(t, channel.ID("c"), principal)

	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)
	defer s.Close()
	s.reconcile() // lastOK = t0

	// Whole-snapshot failure within the lease (t0 + 10s): still served, NOT paused.
	res.set(principal, nil, nil, errors.New("resolver down"))
	clk.advance(10 * time.Second)
	s.reconcile()
	if _, paused := eligPaused(s)["c"]; paused {
		t.Fatal("within T_stale lease the channel must keep serving (not paused)")
	}
	if _, ok := eligRoutes(s)["c"]; !ok {
		t.Fatal("within lease the route must still be published")
	}

	// Past the lease (t0 + 31s): the channel pauses (streaming stops) → business frame
	// maps to unavailable.
	clk.advance(21 * time.Second) // now t0 + 31s
	s.reconcile()
	if _, paused := eligPaused(s)["c"]; !paused {
		t.Fatal("past T_stale the channel must pause (lease expired)")
	}
	if _, ok := eligRoutes(s)["c"]; ok {
		t.Fatal("a paused channel must NOT be a live eligible route")
	}
	if !cap.has("gateway.entitlement.paused") {
		t.Fatal("lease expiry must emit gateway.entitlement.paused (DoD-8 记账)")
	}

	// Recovery: the resolver answers again → the channel resumes (lastOK re-anchored).
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s.reconcile()
	if _, paused := eligPaused(s)["c"]; paused {
		t.Fatal("a recovered resolver must resume the paused channel")
	}
	if _, ok := eligRoutes(s)["c"]; !ok {
		t.Fatal("resumed channel must be a live eligible route again")
	}
	if !cap.has("gateway.entitlement.resumed") {
		t.Fatal("resume must emit gateway.entitlement.resumed (DoD-8 记账)")
	}
}

// TestSweepJustBeforeLeaseDeadlineRearmsAtDeadline is the R2 P1-1 boundary sequence:
// the regular sweep fires one second before lastOK+T_stale, the resolver fails, and
// the running pump must arm its NEXT real timer at the remaining one-second lease
// boundary (not reset a full T_sweep). Advancing the injected clock to exactly that
// boundary pauses the stream without any hand-called reconcile.
func TestSweepJustBeforeLeaseDeadlineRearmsAtDeadline(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const stale = 30 * time.Second
	g := New(Config{
		Resolver:      res,
		Clock:         clk,
		StaleLease:    stale,
		SweepInterval: stale - time.Second,
	})
	const principal = "lease-boundary"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)
	s.StartFeed()
	defer func() {
		s.Close()
		g.pumps.Wait()
	}()

	// StartFeed does one synchronous check; runFeed's initial-dirty does the second and
	// arms the loop timer. Establish that baseline before changing the resolver.
	waitFor(t, func() bool { return res.callCount() >= 2 && clk.armCount() >= 2 },
		"running pump did not complete its initial timer-backed reconcile")
	baselineCalls := res.callCount()
	leaseDeadline := clk.now().Add(stale)
	res.set(principal, nil, nil, errors.New("resolver down")) // no poke

	// First real sweep: t0+29s, still one second inside the lease.
	clk.advance(stale - time.Second)
	waitFor(t, func() bool { return res.callCount() > baselineCalls },
		"real sweep timer did not drive the pre-deadline resolver failure")
	if _, ok := eligRoutes(s)["c"]; !ok {
		t.Fatal("the pre-deadline failure must still ride the remaining lease")
	}
	waitFor(t, func() bool { return clk.lastDeadline().Equal(leaseDeadline) },
		"failed sweep did not re-arm at lastOK+T_stale")

	// The remaining one second is the true upper bound. At the absolute deadline the
	// loop fires and pauses; no T_sweep reset and no direct reconcile call are involved.
	clk.advance(time.Second)
	waitFor(t, func() bool {
		_, paused := eligPaused(s)["c"]
		return paused
	}, "stream remained live beyond lastOK+T_stale")
	if got := clk.now(); !got.Equal(leaseDeadline) {
		t.Fatalf("pause observed at %s, want exact lease deadline %s", got, leaseDeadline)
	}
}

// TestPartialFailureLeaseVsAbsent (spec §3.2 部分失败语义): a per-channel FAILURE ("查得
// 坏消息") rides the T_stale lease then pauses; a channel simply ABSENT from routes
// ("查不到" = confirmed no eligibility) retires immediately. The two must not be
// conflated — a failed channel keeps its subscription for a later resume, an absent one
// does not.
func TestPartialFailureLeaseVsAbsent(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk})
	const principal = "opa"
	h1, id1 := openHome(t, channel.ID("c1"), principal)
	h2, id2 := openHome(t, channel.ID("c2"), principal)

	// Both channels start eligible.
	res.set(principal, []Route{
		memberRoute("c1", h1, id1, clk.now()),
		memberRoute("c2", h2, id2, clk.now()),
	}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)
	defer s.Close()
	s.reconcile()
	if len(s.subs) != 2 {
		t.Fatalf("both channels subscribed, got %d", len(s.subs))
	}

	// c1 → per-channel failure (查得坏消息); c2 → absent (查不到). Within the lease c1
	// stays served; c2 retires immediately.
	res.set(principal, nil, []ChannelFailure{{Channel: "c1", Err: errors.New("c1 query failed")}}, nil)
	clk.advance(5 * time.Second)
	s.reconcile()
	if _, ok := s.subs["c1"]; !ok {
		t.Fatal("a per-channel FAILURE must keep the subscription (lease), not retire it")
	}
	if _, paused := eligPaused(s)["c1"]; paused {
		t.Fatal("c1 within its lease must not be paused yet")
	}
	if _, ok := s.subs["c2"]; ok {
		t.Fatal("a channel absent from routes (查不到) must退订 immediately")
	}

	// Past c1's lease → it pauses (still subscribed, awaiting resume).
	clk.advance(30 * time.Second)
	s.reconcile()
	if _, paused := eligPaused(s)["c1"]; !paused {
		t.Fatal("c1 past its lease must pause")
	}
	if _, ok := s.subs["c1"]; !ok {
		t.Fatal("a paused (failed) channel keeps its subscription for a later resume")
	}
}

// TestWholeSnapshotFailureFirstReconcileUnavailable (P1-2, 六轮终审): the VERY FIRST
// reconcile a session ever does hits a whole-snapshot resolver failure (no prior
// subscription exists for ANY channel — s.subs is empty). Before the fix, an unknown
// channel_id fell through routes/paused into "confirmed absent" → forbidden; but a
// whole-snapshot failure confirms NOTHING, so the correct verdict is unavailable
// (表①: 查询无法完成 → unavailable, not a confirmed-absence forbidden).
func TestWholeSnapshotFailureFirstReconcileUnavailable(t *testing.T) {
	clk := newClock()
	res := newResolver()
	res.set("uma", nil, nil, errors.New("directory query failed"))
	g := New(Config{Resolver: res, Clock: clk})
	s, _ := g.Attach(context.Background(), "uma", nil)
	defer s.Close()

	s.reconcile() // first-ever reconcile; whole-snapshot failure, s.subs is empty.
	if code := codeOf(t, s.Upstream(context.Background(), mkBusiness(t, subjectgate.FrameSubmit, "never-seen"))); code != subjectgate.CodeUnavailable {
		t.Fatalf("a never-subscribed channel after a whole-snapshot failure must be unavailable, not forbidden; got %q", code)
	}
}

// TestNewChannelFailureUnavailableNotForbidden (P1-2, 六轮终审): a channel appears in
// the resolver's per-channel ChannelFailure list on a round where it has NO prior
// subscription (a first-seen / newly failing channel) — before the fix, publishElig
// only recorded `paused` for channels that already had a subscription, so this channel
// fell through to forbidden. It must map to unavailable ("查得坏消息" ≠ "查不到").
func TestNewChannelFailureUnavailableNotForbidden(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const principal = "vic"
	res.set(principal, nil, []ChannelFailure{{Channel: "new-ch", Err: errors.New("query failed")}}, nil)
	g := New(Config{Resolver: res, Clock: clk})
	s, _ := g.Attach(context.Background(), principal, nil)
	defer s.Close()

	s.reconcile()
	if _, ok := s.subs["new-ch"]; ok {
		t.Fatal("a per-channel failure with no prior subscription must not fabricate one")
	}
	if code := codeOf(t, s.Upstream(context.Background(), mkBusiness(t, subjectgate.FrameSubmit, "new-ch"))); code != subjectgate.CodeUnavailable {
		t.Fatalf("a new channel reported as a per-channel failure must be unavailable, not forbidden; got %q", code)
	}
	// A channel that is neither in routes nor in failed remains a confirmed-absence
	// forbidden (control: the fix must not blanket every unknown channel_id).
	if code := codeOf(t, s.Upstream(context.Background(), mkBusiness(t, subjectgate.FrameSubmit, "truly-unknown"))); code != subjectgate.CodeForbidden {
		t.Fatalf("a channel absent from BOTH routes and failed must stay forbidden; got %q", code)
	}
}

// TestStartFeedSyncEligibility (spec §3.2 StartFeed synchronous first reconcile /
// connect-then-act): after StartFeed returns, eligibility is ALREADY resolved (the
// synchronous first reconcile), so a client acting immediately does not race the pump's
// first async圈.
func TestStartFeedSyncEligibility(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk})
	const principal = "pat"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)

	s.StartFeed()
	// Immediately after StartFeed the资格账 already carries the route (synchronous
	// reconcile happened before the pump goroutine launched).
	if _, ok := eligRoutes(s)["c"]; !ok {
		t.Fatal("StartFeed must resolve eligibility synchronously (connect-then-act)")
	}
	g.Close()
}

// TestReconcileNoResolverForbidsAll: with no resolver injected, reconcile publishes an
// empty资格账 (every business frame forbidden — no channel is ever eligible).
func TestReconcileNoResolverForbidsAll(t *testing.T) {
	g := New(Config{Clock: newClock()}) // no Resolver
	s, _ := g.Attach(context.Background(), "q", nil)
	defer s.Close()
	s.reconcile()
	if len(eligRoutes(s)) != 0 {
		t.Fatal("no resolver → empty资格账 (all forbidden)")
	}
}
