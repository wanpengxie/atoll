package gateway

// 统一会话闸 + 关站全序 barriers (spec §3.2, DoD-7②⑨⑩): late-start pump refusal,
// blocked-delivery unblock-on-Close, close-gate frame refusal, concurrent Close gating,
// and resolver-enumeration × Close bounded return (the gateway half of DoD-7⑥).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	g := New(Config{Resolver: res, Clock: newClock()})
	s, err := g.Attach(context.Background(), "late", nil)
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

// TestBlockedDeliverUnblocksOnClose (DoD-7⑩ 阻塞递交 + Close 等在途归零): a delivery that
// has passed the eligibility check and is blocked inside Deliver (interpreter attached
// but not replying) unblocks when Gateway.Close cancels the session ctx; the upstream
// call returns a closed frame with bounded latency, and Close itself returns only AFTER
// the in-flight delivery drains (delivering→0).
func TestBlockedDeliverUnblocksOnClose(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := New(Config{Resolver: res, Clock: clk})

	const principal = "gwen"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(context.Background(), principal, nil)
	s.reconcile() // establish eligibility (member route for c)
	slot, _ := h.SubjectSlotFor(id)

	got := make(chan struct{}, 1)
	release := make(chan struct{})
	stop := blockingInterpreter(slot, got, release)
	defer stop()

	upstreamDone := make(chan subjectgate.Frame, 1)
	go func() {
		upstreamDone <- s.Upstream(context.Background(), mkBusiness(t, subjectgate.FrameSubmit, "c"))
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

// TestCloseGateRefusesFrame (DoD-7② 关站闩真交错): Upstream has already parsed the
// frame, extracted channel_id, and selected a valid member route, but a failpoint parks
// it immediately BEFORE the delivery permit. Concurrent Gateway.Close sets the session
// latch; releasing the parsed frame then makes beginDeliver refuse it as closed. This is
// the read-before-permit interleave, not a sequential "Close then call Upstream" proxy.
func TestCloseGateRefusesFrame(t *testing.T) {
	res := newResolver()
	g := New(Config{Resolver: res, Clock: newClock()})
	s, _ := g.Attach(context.Background(), "hank", nil)
	// A member route in the资格账 (nil Home is never dereferenced — beginDeliver refuses
	// first once closed).
	s.elig.Store(&eligState{
		routes: map[channel.ID]Route{"c": {Channel: "c", Access: AccessMember}},
		paused: map[channel.ID]struct{}{},
	})
	parsed := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.beforeDeliver = func() {
		close(parsed)
		<-release
	}
	frame := mkBusiness(t, subjectgate.FrameSubmit, "c")
	result := make(chan subjectgate.Frame, 1)
	go func() {
		result <- s.Upstream(context.Background(), frame)
	}()
	select {
	case <-parsed:
	case <-time.After(2 * time.Second):
		t.Fatal("frame never reached the read-before-delivery-permit failpoint")
	}

	closeDone := make(chan struct{})
	go func() { _ = g.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		// No permit existed yet, so Close has no delivery to drain and may return.
	case <-time.After(2 * time.Second):
		t.Fatal("Gateway.Close did not set the latch while the parsed frame was pre-permit")
	}
	close(release)
	select {
	case got := <-result:
		if code := codeOf(t, got); code != subjectgate.CodeClosed {
			t.Fatalf("pre-permit frame released after Close must be closed, got %q", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pre-permit frame did not return after failpoint release")
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

func (r *barrierResolver) Snapshot(ctx context.Context, principal string) ([]Route, []ChannelFailure, error) {
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})
	return r.routes, nil, nil
}

// TestConcurrentCloseGated (关站 idempotent + 单 teardown gating): two concurrent Close
// calls BOTH enter Close (proven by the closeEntered handshake seam); the presence loop
// is parked inside a blocking resolver Snapshot, holding the first Close at
// presenceWG.Wait; NEITHER returns until the barrier releases — the sync.Once teardown
// gates every caller. After release both return, coverage is cleaned, pumps joined.
func TestConcurrentCloseGated(t *testing.T) {
	br := &barrierResolver{entered: make(chan struct{}), release: make(chan struct{})}
	g := New(Config{Resolver: br, Clock: newClock()})

	// Seat a device so the presence loop has a principal to enumerate, then Start it.
	if _, err := g.Attach(context.Background(), "iris", nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if err := g.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The loop's first 圈 calls Snapshot and blocks.
	select {
	case <-br.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("presence loop never entered the resolver")
	}

	// Handshake seam: count real entries into Close.
	var entered int32
	enteredCh := make(chan struct{}, 2)
	g.closeEntered = func() {
		atomic.AddInt32(&entered, 1)
		enteredCh <- struct{}{}
	}

	returned := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		n := i
		go func() { _ = g.Close(); returned <- n }()
	}
	<-enteredCh
	<-enteredCh
	if got := atomic.LoadInt32(&entered); got != 2 {
		t.Fatalf("both Close calls must enter Close, got %d", got)
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
	g := New(Config{Resolver: res, Clock: newClock()})
	s, err := g.Attach(context.Background(), "direct-close", nil)
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

func (r *hangingResolver) Snapshot(ctx context.Context, principal string) ([]Route, []ChannelFailure, error) {
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
	g := New(Config{Resolver: hr, Clock: newClock(), PumpJoinTimeout: 50 * time.Millisecond})
	var starts atomic.Int64
	startDone := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		s, err := g.Attach(context.Background(), fmt.Sprintf("leak-%d", i), nil)
		if err != nil {
			t.Fatalf("Attach %d: %v", i, err)
		}
		s.feedStarted = func() { starts.Add(1) }
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
	if got := g.LeakedPumps(); got != n {
		t.Fatalf("Close must count leaked pumps, not timeout incidents: got %d want %d", got, n)
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
	if got := starts.Load(); got != 0 {
		t.Fatalf("%d runFeed goroutine(s) started after Close returned; want zero", got)
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
		g := New(Config{Resolver: res, Clock: clk})
		const principal = "jack"
		h, id := openHome(t, channel.ID("c"), principal)
		res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
		s, _ := g.Attach(context.Background(), principal, nil)
		if err := g.Start(); err != nil { // presence loop enumerates
			t.Fatalf("Start: %v", err)
		}
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
