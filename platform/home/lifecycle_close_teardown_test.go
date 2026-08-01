package home

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Close is a one-shot barrier with several callers and no owner privileges:
// whoever gets there first performs the single teardown, everyone else waits
// for it, and what any of them observes afterwards must be the same. The three
// tests here hold a real Close inside its real teardown block and ask what the
// rest of the world sees from there — a second caller, a mutation attempt, a
// durable timer coming due.
//
// The hold is taken at a genuine production boundary rather than a test hook:
// the teardown's first act is to stop the reconcile loop and WAIT for it, so a
// reconcile pass parked inside the IntroductionResolver (an ordinary injected
// dependency) parks the closer with it, at the top of the teardown block, with
// every organ still alive.

// lifecycleReconcileGate is that dependency. Disarmed it answers "no such
// declaration", which is the quiet skip; armed it parks the reconcile pass
// until the test lets go.
type lifecycleReconcileGate struct {
	armed     atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	openOnce  sync.Once
}

func newLifecycleReconcileGate() *lifecycleReconcileGate {
	return &lifecycleReconcileGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *lifecycleReconcileGate) arm()    { g.armed.Store(true) }
func (g *lifecycleReconcileGate) unpark() { g.openOnce.Do(func() { close(g.release) }) }

func (g *lifecycleReconcileGate) ResolveDeclaration(
	context.Context, channel.ID, string,
) (channelspec.DeclarationFacts, error) {
	if g.armed.Load() {
		g.enterOnce.Do(func() { close(g.entered) })
		<-g.release
	}
	return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
}

func (g *lifecycleReconcileGate) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}

// lifecycleCloseConfig is a bootstrap channel carrying ONE declared agent —
// the declared instance is what gives the reconcile pass something to resolve,
// which is what makes the gate above reachable.
func lifecycleCloseConfig(
	t *testing.T,
	name string,
	composition CompositionResolver,
	gate *lifecycleReconcileGate,
) Config {
	t.Helper()
	return Config{
		ChannelID:             channel.ID("lifecycle-" + name),
		DBPath:                filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:   composition,
		IntroductionResolver:  gate,
		ReconcileInterval:     time.Hour,
		Bootstrap:             true,
		BootstrapDeclarations: []DeclareRequest{restartTimerDeclaration()},
	}
}

// parkCloseInsideTeardown starts one Close, waits until the reconcile pass it
// is joining is parked, and returns that Close's result channel. On return the
// closer is inside the teardown block with every organ still alive.
func parkCloseInsideTeardown(
	t *testing.T,
	h *Home,
	gate *lifecycleReconcileGate,
	reason string,
) <-chan error {
	t.Helper()
	gate.arm()
	h.pokeReconcile()
	restartRecv(t, "the reconcile pass to reach the parked resolver", gate.entered)

	result := make(chan error, 1)
	go func() { result <- h.closeInternalWithin(reason, restartWaitBudget) }()
	restartEventually(t, "the close gate to be published", func() bool { return h.closed.Load() })
	assertTeardownUnfinished(t, h)
	return result
}

func assertTeardownUnfinished(t *testing.T, h *Home) {
	t.Helper()
	select {
	case <-h.closeDone:
		t.Fatal("the teardown block completed while the reconcile join was still parked")
	default:
	}
}

// lifecycleObserveWithin polls cond and reports whether it ever held. Unlike
// restartEventually it is not an assertion: it is used where BOTH answers are
// legal outcomes and the test needs to know which one happened.
func lifecycleObserveWithin(budget time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(budget)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(restartPollEvery)
	}
}

func lifecycleCountRowsOfType(t *testing.T, query storespec.MessageQuery, typ string) int {
	t.Helper()
	rows, err := query.ReadAfterSeq(context.Background(), 0, 1000)
	if err != nil {
		t.Fatalf("read the channel log: %v", err)
	}
	count := 0
	for _, row := range rows {
		if row.Envelope.Type == typ {
			count++
		}
	}
	return count
}

// Eight callers, one teardown. "No error from any of them" is the weak half of
// this: the strong half is that the teardown block itself ran exactly ONCE
// (its closing event is logged once, and stays once after a ninth sequential
// Close), and that not one caller returned while the single owner was still
// inside it.
func TestConcurrentCloseRunsExactlyOneTeardownAndEveryCallerWaitsForIt(t *testing.T) {
	gate := newLifecycleReconcileGate()
	cfg := lifecycleCloseConfig(t, "concurrent-close", newRestartTimerFixture(false), gate)
	handler := newLifecycleLogProbe("", nil)
	cfg.Logger = slog.New(handler)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	owner := parkCloseInsideTeardown(t, h, gate, "owner")

	const followers = 7
	results := make(chan error, followers)
	launched := make(chan struct{}, followers)
	for range followers {
		go func() {
			launched <- struct{}{}
			results <- h.closeInternalWithin("follower", restartWaitBudget)
		}()
	}
	for range followers {
		restartRecv(t, "every follower to call Close", launched)
	}
	// All eight callers are now in Close and the owner is parked, so the barrier
	// has not opened. Any follower that returned now would have returned WITHOUT
	// the teardown having happened.
	assertTeardownUnfinished(t, h)
	select {
	case err := <-results:
		t.Fatalf("a follower returned (%v) before the single teardown completed", err)
	default:
	}
	if got := handler.count("platform.home.closed"); got != 0 {
		t.Fatalf("teardown completions before the park was released = %d", got)
	}

	gate.unpark()
	if err := restartRecv(t, "the teardown owner to return", owner); err != nil {
		t.Fatalf("owner Close: %v", err)
	}
	for range followers {
		if err := restartRecv(t, "a waiting Close to return", results); err != nil {
			t.Fatalf("follower Close: %v", err)
		}
	}
	if got := handler.count("platform.home.closed"); got != 1 {
		t.Fatalf("teardown ran %d times under 8 concurrent callers, want exactly 1", got)
	}
	// The single-ownership claim is about the barrier, not about a race window:
	// a later, uncontended caller must still find the teardown spent.
	if err := h.closeInternalWithin("late", restartWaitBudget); err != nil {
		t.Fatalf("sequential Close after the concurrent ones: %v", err)
	}
	if got := handler.count("platform.home.closed"); got != 1 {
		t.Fatalf("a late Close re-ran the teardown: %d completions", got)
	}
}

// T14. A panic inside the teardown block belongs to the caller that was
// running it. It must not be handed to the other waiters, and — the part that
// decides whether the process survives — it must not leave the barrier shut:
// closeDone is closed on the way out, the waiter is released, and the waiter
// finishes the retryable tail (the store close) the panicking owner skipped.
func TestTeardownPanicStaysWithItsOwnerAndStillOpensTheBarrier(t *testing.T) {
	gate := newLifecycleReconcileGate()
	cfg := lifecycleCloseConfig(t, "teardown-panic", newRestartTimerFixture(false), gate)
	handler := newLifecycleLogProbe("platform.home.closed", "teardown-panic")
	cfg.Logger = slog.New(handler)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	gate.arm()
	h.pokeReconcile()
	restartRecv(t, "the reconcile pass to reach the parked resolver", gate.entered)

	owner := make(chan any, 2)
	go func() {
		defer func() { owner <- recover() }()
		_ = h.closeInternalWithin("panicking-owner", restartWaitBudget)
		owner <- nil
	}()
	restartEventually(t, "the close gate to be published", func() bool { return h.closed.Load() })
	assertTeardownUnfinished(t, h)

	waiter := make(chan error, 1)
	waiterLaunched := make(chan struct{})
	go func() {
		close(waiterLaunched)
		waiter <- h.closeInternalWithin("waiter", restartWaitBudget)
	}()
	restartRecv(t, "the waiting caller to call Close", waiterLaunched)

	gate.unpark()
	if got := restartRecv(t, "the teardown owner to unwind", owner); got != "teardown-panic" {
		t.Fatalf("owner recovered %v, want the teardown panic itself", got)
	}
	if err := restartRecv(t, "the waiter to be released", waiter); err != nil {
		t.Fatalf("waiter Close was infected by the owner's panic: %v", err)
	}
	select {
	case <-h.closeDone:
	default:
		t.Fatal("the panicking teardown left the barrier shut")
	}
	if !h.storeCloseDone.Load() {
		t.Fatal("nobody finished the store close after the teardown panic")
	}
}

// T13. A durable timer that comes due while the channel is closing is the one
// race where "it fired" and "it did not fire" are BOTH correct — the engine is
// still alive at the top of the teardown block, so whether the fire commits
// before the engine stops is a genuine race. What is not negotiable is the
// count across the whole close/reopen boundary: exactly one fire, never zero
// (lost) and never two (replayed on top of a committed one). The same parked
// window also proves the mutation gate is already shut while the teardown tail
// has not even started.
func TestDueTimerAcrossTheCloseWindowFiresExactlyOnce(t *testing.T) {
	gate := newLifecycleReconcileGate()
	clock := newRestartShiftClock()
	first := newRestartTimerFixture(true)
	cfg := lifecycleCloseConfig(t, "close-window", first, gate)
	cfg.Clock = clock
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	armed := restartRecv(t, "the durable timer to be armed", first.armed)
	if armed.err != "" || armed.timerID == "" {
		t.Fatalf("After(TimerHomeDurable) = %q err=%s", armed.timerID, armed.err)
	}
	restartRecv(t, "the arming body to reach its mailbox", first.started)
	if _, found, err := h.query.LatestBySenderAndType(
		ctx, armed.actorID, restartTimerType); err != nil || found {
		t.Fatalf("the timer fired before the close window opened: found=%v err=%v", found, err)
	}

	closed := parkCloseInsideTeardown(t, h, gate, "close-window")

	// The mutation gate stands BEFORE the teardown tail, not after it: while the
	// closer is still parked at the top of the block, member words are already
	// refused with the retryable channel-unavailable verdict.
	if _, err := admitThroughSysOp(h, ctx, actor.KindHuman, "late-joiner"); !isChannelUnavailableForTest(err) {
		t.Fatalf("Admit inside the close window = %v, want channel_unavailable", err)
	}
	if err := removeThroughSysOp(h, ctx, armed.actorID); !isChannelUnavailableForTest(err) {
		t.Fatalf("Remove inside the close window = %v, want channel_unavailable", err)
	}

	// Now make it due, inside the window.
	clock.jump(2 * time.Hour)
	firedInWindow := lifecycleObserveWithin(3*time.Second, func() bool {
		_, found, err := h.query.LatestBySenderAndType(ctx, armed.actorID, restartTimerType)
		return err == nil && found
	})
	t.Logf("due timer committed inside the close window: %v", firedInWindow)

	gate.unpark()
	if err := restartRecv(t, "the parked Close to return", closed); err != nil {
		t.Fatalf("Close over a due timer: %v", err)
	}

	// Second life. It declares the same class but arms nothing, so anything that
	// fires here can only be the row the first life left behind.
	second := newRestartTimerFixture(false)
	secondClock := newRestartShiftClock()
	reopen := cfg
	reopen.CompositionResolver = second
	reopen.IntroductionResolver = inertIntroductionResolver{}
	reopen.Clock = secondClock
	reopen.Bootstrap = false
	reopen.BootstrapDeclarations = nil
	reopen.MustExistDB = true
	h2, err := Open(reopen)
	if err != nil {
		t.Fatalf("reopen after closing over a due timer: %v", err)
	}
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	restartRecv(t, "the restored body to reach its mailbox", second.started)
	select {
	case unexpected := <-second.armed:
		t.Fatalf("the second life armed its own timer %q — the count would prove nothing",
			unexpected.timerID)
	default:
	}
	secondClock.jump(2 * time.Hour)

	restartEventually(t, "the timer to have fired exactly once across the close", func() bool {
		return lifecycleCountRowsOfType(t, h2.query, restartTimerType) == 1
	})
	// Once the durable row is gone there is nothing left that could fire again,
	// so the count above is final rather than merely current.
	timers := openRestartTimerStoreReader(t, cfg.ChannelID, cfg.DBPath)
	restartEventually(t, "the durable timer row to be retired", func() bool {
		_, pending, err := timers.NextFireAt(ctx)
		return err == nil && !pending
	})
	if got := lifecycleCountRowsOfType(t, h2.query, restartTimerType); got != 1 {
		t.Fatalf("fire rows after the durable row was retired = %d, want exactly 1", got)
	}
	row, found, err := h2.query.LatestBySenderAndType(ctx, armed.actorID, restartTimerType)
	if err != nil || !found {
		t.Fatalf("the one fire row: found=%v err=%v", found, err)
	}
	if row.Envelope.ID != message.ID("timer:"+string(armed.timerID)) {
		t.Fatalf("fire envelope id = %q, want the deterministic id of the armed timer %q",
			row.Envelope.ID, armed.timerID)
	}
}
