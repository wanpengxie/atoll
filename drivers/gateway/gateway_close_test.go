package gateway

// 统一会话闸 + 关站全序 barriers (spec §3.2, DoD-7②⑨⑩): late-start pump refusal,
// blocked-delivery unblock-on-Close, close-gate frame refusal, concurrent Close gating,
// and resolver-enumeration × Close bounded return (the gateway half of DoD-7⑥).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestLateStartFeedRefused (DoD-7⑨ 迟启泵 barrier): a session that seats, then the
// gateway Closes, then calls StartFeed → the pump is refused (beginFeed sees closed)
// and the session is torn down, never left reading a closing Home.
func TestLateStartFeedRefused(t *testing.T) {
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: newClock()})
	s, err := g.Attach("late", nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// tryRegisterPump (gateway half of beginFeed) must refuse after Close.
	if g.tryRegisterPump() {
		t.Fatal("tryRegisterPump must refuse after Close (迟启泵)")
	}
	// StartFeed on the closed gateway tears the session down (no pump left running).
	s.StartFeed()
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("late StartFeed must tear the session down")
	}
}

func TestLifecycleOwnerMisuseFailsLoud(t *testing.T) {
	t.Run("double Start", func(t *testing.T) {
		g := newTestGateway(t, Config{Resolver: newResolver()}, settings{clock: newClock()})
		g.Start()
		defer g.Close()
		mustPanic(t, g.Start)
	})
	t.Run("Start after Close", func(t *testing.T) {
		g := newTestGateway(t, Config{Resolver: newResolver()}, settings{clock: newClock()})
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		mustPanic(t, g.Start)
	})
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected lifecycle misuse to panic")
		}
	}()
	fn()
}

// TestBlockedDeliverUnblocksOnClose (DoD-7⑩ 阻塞递交 + Close 等在途归零): a delivery that
// has passed the eligibility check and is blocked inside Deliver (interpreter attached
// but not replying) unblocks when Gateway.Close cancels the session ctx; the upstream
// call returns a closed frame with bounded latency, and Close itself returns only AFTER
// the in-flight delivery drains (delivering→0).
func TestBlockedDeliverUnblocksOnClose(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})

	const principal = "gwen"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	s.reconcile() // establish eligibility (member route for c)
	slot, _ := h.SubjectSlotFor(id)

	got := make(chan struct{}, 1)
	release := make(chan struct{})
	stop := blockingInterpreter(slot, got, release)
	defer stop()

	upstreamDone := make(chan subjectgate.Frame, 1)
	go func() {
		upstreamDone <- s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))
	}()
	// Wait until the delivery is in-flight (interpreter received the job, now blocked).
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery never reached the interpreter")
	}

	// Close: cancels the session ctx → the blocked Deliver unblocks → Upstream returns.
	closeDone := make(chan struct{})
	go func() { _ = g.Close(); close(closeDone) }()

	select {
	case f := <-upstreamDone:
		if got := codeOf(t, f); got != subjectgate.CodeClosed {
			t.Fatalf("blocked delivery unblocked by Close must map to closed, got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked delivery did not unblock on Close (纪律④/统一会话闸 broken)")
	}
	// Close returns only after the in-flight delivery drained (delivering→0).
	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after in-flight delivery drained")
	}
}

// TestCloseSealsAllSessionsBeforePresenceJoin is the P0 regression: a real presence
// resolver parks the presence loop, then Close begins. Every session in the frozen
// snapshot — including multiple devices for one principal and another principal — must
// be sealed before Close reaches the blocked presence join.
func TestCloseSealsAllSessionsBeforePresenceJoin(t *testing.T) {
	br := &barrierResolver{entered: make(chan struct{}), release: make(chan struct{})}
	g := newTestGateway(t, Config{Resolver: br}, settings{clock: newClock()})
	var sessions []*Session
	for _, principal := range []string{"hank", "hank", "jill"} {
		s, err := g.Attach(principal, nil)
		if err != nil {
			t.Fatalf("Attach(%s): %v", principal, err)
		}
		s.elig.Store(&eligState{
			routes: map[channel.ID]Route{"c": {Channel: "c", Access: AccessMember}},
			paused: map[channel.ID]struct{}{},
		})
		sessions = append(sessions, s)
	}
	g.Start()
	select {
	case <-br.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("presence loop never entered the blocking resolver")
	}

	closeDone := make(chan struct{})
	go func() { _ = g.Close(); close(closeDone) }()
	deadline := time.After(2 * time.Second)
	for i, s := range sessions {
		select {
		case <-s.Done():
		case <-deadline:
			t.Fatalf("Close did not seal session %d/%d before the blocked presence join", i+1, len(sessions))
		}
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned while the real presence barrier was still held")
	default:
	}
	for i, s := range sessions {
		if code := codeOf(t, s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))); code != subjectgate.CodeClosed {
			t.Fatalf("frame for sealed session %d must be closed, got %q", i+1, code)
		}
	}
	close(br.release)
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the presence barrier released")
	}
}

// barrierResolver blocks its FIRST Snapshot on `release` (signalling `entered` first),
// so a test can park the presence loop inside enumeration and hold Gateway.Close at
// presenceWG.Wait — a real teardown barrier (Snapshot ignores ctx, so the loop only
// exits AFTER Snapshot returns, exactly like a slow app-side membership query).
type barrierResolver struct {
	entered chan struct{}
	release chan struct{}
	routes  []Route
	once    sync.Once
}

func (r *barrierResolver) Snapshot(ctx context.Context, principal string) ([]Route, []channel.ID, error) {
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})
	return r.routes, nil, nil
}

// TestConcurrentCloseGated (关站 idempotent + 单 teardown gating): two concurrent Close
// calls start from one test-side barrier; the presence loop
// is parked inside a blocking resolver Snapshot, holding the first Close at
// presenceWG.Wait; NEITHER returns until the barrier releases — the sync.Once teardown
// gates every caller. After release both return, coverage is cleaned, pumps joined.
func TestConcurrentCloseGated(t *testing.T) {
	br := &barrierResolver{entered: make(chan struct{}), release: make(chan struct{})}
	g := newTestGateway(t, Config{Resolver: br}, settings{clock: newClock()})

	// Seat a device so the presence loop has a principal to enumerate, then Start it.
	s, err := g.Attach("iris", nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	g.Start()
	// The loop's first 圈 calls Snapshot and blocks.
	select {
	case <-br.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("presence loop never entered the resolver")
	}

	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	returned := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		n := i
		go func() {
			ready.Done()
			<-start
			_ = g.Close()
			returned <- n
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the single teardown never sealed its session set")
	}

	// Barrier held (presence loop parked in Snapshot): NEITHER Close may return.
	select {
	case n := <-returned:
		t.Fatalf("Close %d returned while the presence loop was still parked in Snapshot", n)
	case <-time.After(100 * time.Millisecond):
	}

	// Release → Snapshot returns → the loop sees presenceCtx cancelled → exits →
	// presenceWG done → the single teardown completes → BOTH Close return.
	close(br.release)
	for i := 0; i < 2; i++ {
		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("a Close did not return after the barrier released")
		}
	}
	if len(g.coverage) != 0 {
		t.Fatalf("Close must clean coverage, got %d entries", len(g.coverage))
	}
	g.pumps.Wait()
}

// TestSessionClosedThenStartFeedRefused (P1-3, 会话闸 session half): a session closed
// DIRECTLY (s.Close(), not via Gateway.Close — the gateway itself stays open) must
// refuse a late beginFeed. Before the fix, beginFeed only consulted the GATEWAY's
// closed flag (g.mu) — a session that was already torn down by its own connector
// (e.g. a failed receipt write calling s.Close()) would still pass that check on a
// still-open gateway and register a pump for a dead session.
func TestSessionClosedThenStartFeedRefused(t *testing.T) {
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: newClock()})
	s, err := g.Attach("direct-close", nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	s.Close() // session closed directly; the gateway is untouched.
	if g.closed {
		t.Fatal("this test exercises the SESSION half of the gate — the gateway must stay open")
	}
	if s.beginFeed() {
		t.Fatal("beginFeed must refuse once the SESSION itself is closed (统一会话闸 session half)")
	}
}

// hangingResolver signals `entered` on its first Snapshot call, then blocks on
// `release` FOREVER — it deliberately ignores ctx, modelling a badly-behaved app-side
// resolver (or a stuck DB call) that a bounded ctx timeout cannot rescue.
type hangingResolver struct {
	entered chan struct{}
	release chan struct{}
}

func (r *hangingResolver) Snapshot(ctx context.Context, principal string) ([]Route, []channel.ID, error) {
	r.entered <- struct{}{}
	<-r.release
	return nil, nil, nil
}

// TestClosePumpJoinBounded (R2 P2-1, §2.1 #11/#12): N sessions register pumps, then
// all N park in StartFeed's synchronous resolver before their goroutines can launch.
// Close returns within its bound and records exactly N leaked pumps (not one timeout
// incident). When the resolver is later released, every StartFeed retires its register
// at the post-reconcile latch check: zero runFeed goroutines start after Close returned.
func TestClosePumpJoinBounded(t *testing.T) {
	const n = 3
	hr := &hangingResolver{entered: make(chan struct{}, n), release: make(chan struct{})}
	defer func() {
		select {
		case <-hr.release:
		default:
			close(hr.release)
		}
	}()
	clk := newClock()
	cap := &logCapture{}
	g := newTestGateway(t, Config{Resolver: hr, Logger: cap.logger()}, settings{clock: clk, pumpJoinTimeout: 50 * time.Millisecond})
	startDone := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		s, err := g.Attach(fmt.Sprintf("leak-%d", i), nil)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		go func() {
			s.StartFeed()
			startDone <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-hr.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("StartFeed %d never reached the blocking resolver", i)
		}
	}
	if got := g.registeredPumps.Load(); got != n {
		t.Fatalf("registered pump count before Close = %d, want %d", got, n)
	}
	closeDone := make(chan struct{})
	go func() { _ = g.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within the bounded pump-join budget (P1-3 leak accounting broken)")
	}
	if got, ok := cap.intAttr("platform.gateway.pump_join_timeout", "leaked"); !ok || got != n {
		t.Fatalf("pump timeout log leaked = %d, %v; want %d, true", got, ok, n)
	}

	// Close has returned. A resolver that finally wakes must not create late-owned
	// goroutines against already-closing Homes.
	close(hr.release)
	for i := 0; i < n; i++ {
		select {
		case <-startDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("released StartFeed %d did not retire", i)
		}
	}
	waitFor(t, func() bool { return g.registeredPumps.Load() == 0 },
		"released blocked registrations did not retire to zero")
	if got := clk.armCount(); got != 0 {
		t.Fatalf("late resolver return armed %d feed timers after Close; want zero", got)
	}
}

// TestResolverEnumerationConcurrentWithClose (DoD-7⑥ gateway half): the resolver
// enumeration LOOPS (presence reconcile loop via Start + the session read pump via
// StartFeed) racing Gateway.Close all return bounded, no panic, no half-open state —
// Close cancels the presence/session ctx mid-enumeration and JOINS both goroutines
// (presenceWG.Wait / pumps.Wait). Many iterations under -race. (The reconcile functions
// are loop-private — a test never calls them concurrently with Close; only the loops
// Close joins may touch coverage/subs.)
func TestResolverEnumerationConcurrentWithClose(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		clk := newClock()
		res := newResolver()
		g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
		const principal = "jack"
		h, id := openHome(t, channel.ID("c"), principal)
		res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
		s, _ := g.Attach(principal, nil)
		g.Start()        // presence loop enumerates
		s.StartFeed()    // read pump enumerates
		g.kickPresence() // ensure the presence loop is actively re-enumerating
		s.markDirty()    // ensure the pump is actively re-resolving

		done := make(chan struct{})
		go func() { _ = g.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: enumeration loops × Close did not return bounded", iter)
		}
	}
}
