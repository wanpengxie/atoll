package actorrt

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// recordActor records the order and concurrency of Receive invocations WITHOUT
// any internal lock on the per-message fields — proving the substrate
// serializes for it.
type recordActor struct {
	seen        []string
	inFlight    int
	maxParallel int

	startedCh chan struct{}
	stoppedCh chan struct{}
	receive   func()
}

func newRecordActor() *recordActor {
	return &recordActor{
		startedCh: make(chan struct{}, 1),
		stoppedCh: make(chan struct{}, 1),
	}
}

func (a *recordActor) Start(ctx context.Context, self ActorContext) error {
	select {
	case a.startedCh <- struct{}{}:
	default:
	}
	return nil
}

func (a *recordActor) Stop(ctx context.Context) error {
	select {
	case a.stoppedCh <- struct{}{}:
	default:
	}
	return nil
}

func (a *recordActor) Receive(ctx context.Context, env *message.Envelope) error {
	a.inFlight++
	if a.inFlight > a.maxParallel {
		a.maxParallel = a.inFlight
	}
	a.seen = append(a.seen, string(env.ID))
	if a.receive != nil {
		a.receive()
	}
	a.inFlight--
	return nil
}

func env(id string) *message.Envelope {
	return &message.Envelope{ID: message.ID(id)}
}

// static adapts a prebuilt Actor to the two-phase Spawn build closure (the
// incarnation is ignored — these tests do not weld a livePen).
func static(impl Actor) func(Incarnation) Actor {
	return func(Incarnation) Actor { return impl }
}

// mustDeliver delivers to a single actor and fails on a non-Delivered outcome.
func mustDeliver(t *testing.T, rt *Runtime, id actor.ActorID, e *message.Envelope) {
	t.Helper()
	res, err := rt.deliver([]actor.ActorID{id}, e)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per[id]; got != Delivered {
		t.Fatalf("deliver outcome = %v, want Delivered", got)
	}
}

func TestCellSerialDelivery(t *testing.T) {
	t.Parallel()
	done := make(chan struct{}, 100)
	a := newRecordActor()
	a.receive = func() {
		time.Sleep(time.Millisecond)
		done <- struct{}{}
	}
	rt, _ := New(Config{Parent: context.Background(), Mailbox: 200})
	rt.Spawn("a", actor.KindAgent, static(a))

	const n = 50
	for i := 0; i < n; i++ {
		mustDeliver(t, rt, "a", env(string(rune('A'+i%26))))
	}
	for i := 0; i < n; i++ {
		<-done
	}
	rt.StopAll()
	if a.maxParallel != 1 {
		t.Fatalf("maxParallel = %d, want 1 (substrate must serialize)", a.maxParallel)
	}
	if len(a.seen) != n {
		t.Fatalf("seen %d, want %d", len(a.seen), n)
	}
}

func TestCellStartStop(t *testing.T) {
	t.Parallel()
	a := newRecordActor()
	rt, _ := New(Config{Parent: context.Background()})
	inc := rt.Spawn("a", actor.KindAgent, static(a))
	select {
	case <-a.startedCh:
	case <-time.After(time.Second):
		t.Fatal("Start never ran")
	}
	rt.Despawn(inc)
	select {
	case <-a.stoppedCh:
	case <-time.After(time.Second):
		t.Fatal("Stop never ran after Despawn")
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still present after Despawn")
	}
}

func TestCellMailboxFull(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	a := newRecordActor()
	a.receive = func() { <-block }
	rt, _ := New(Config{Parent: context.Background(), Mailbox: 1})
	defer func() { close(block); rt.StopAll() }()
	rt.Spawn("a", actor.KindAgent, static(a))

	var gotFull atomic.Bool
	for i := 0; i < 50; i++ {
		res, err := rt.deliver([]actor.ActorID{"a"}, env("x"))
		if err != nil {
			t.Fatalf("deliver: %v", err)
		}
		if res.Per["a"] == MailboxFull {
			gotFull.Store(true)
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !gotFull.Load() {
		t.Fatal("expected MailboxFull once mailbox saturated, never got it")
	}
}

// TestDeliverNotHosted: the substrate reports NotHosted (not a silent
// nil-success) for an audience member it does not host, so the seam can
// fast-fail receiver_unavailable. (B4)
func TestDeliverNotHosted(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	res, err := rt.deliver([]actor.ActorID{"ghost"}, env("x"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per["ghost"]; got != NotHosted {
		t.Fatalf("outcome = %v, want NotHosted", got)
	}
}

type panicActor struct{}

func (panicActor) Receive(ctx context.Context, env *message.Envelope) error { panic("boom") }

type startPanicActor struct{}

func (startPanicActor) Receive(ctx context.Context, env *message.Envelope) error { return nil }
func (startPanicActor) Start(ctx context.Context, self ActorContext) error       { panic("start boom") }

// recordingWatcher is the consumer end of the obs embodiment-push channel: it
// records each death (DELETED edge) the runtime publishes.
type recordingWatcher struct {
	mu     sync.Mutex
	downs  []actor.ActorID
	notify chan struct{}
}

func (w *recordingWatcher) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	w.mu.Lock()
	w.downs = append(w.downs, id)
	w.mu.Unlock()
	if w.notify != nil {
		w.notify <- struct{}{}
	}
}

func TestCellPanicPublishesDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w) // register BEFORE spawn — no edge missed
	rt.Spawn("a", actor.KindAgent, static(panicActor{}))
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge after actor panic")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.downs) != 1 || w.downs[0] != "a" {
		t.Fatalf("downs = %+v, want one for actor a", w.downs)
	}
	// Self-eviction: the dead instance is unaddressable WITHOUT the watcher
	// despawning it (OnDown runs AFTER eviction). (B1)
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("dead cell still addressable — did not self-evict")
	}
}

// despawningWatcher reproduces the DANGEROUS legacy pattern (despawn in the death
// reaction). It must NOT deadlock: the cell self-evicts before OnDown, so the
// guarded Despawn(inc) finds a pointer mismatch and no-ops instead of self-joining
// the dying goroutine. It holds the incarnation handle (the guarded Despawn API
// requires it — a watcher can no longer despawn by bare id). (B1 regression)
type despawningWatcher struct {
	rt     *Runtime
	inc    Incarnation
	notify chan struct{}
}

func (w *despawningWatcher) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	w.rt.Despawn(w.inc)
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func TestPanicDeathWithDespawningWatcherDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	w := &despawningWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	w.rt = rt
	rt.WatchDown(w)
	// Receive-panic (not Start-panic) so the incarnation handle is recorded BEFORE
	// death is triggered by the deliver below — no race on w.inc.
	w.inc = rt.Spawn("a", actor.KindAgent, static(panicActor{}))
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("death path deadlocked (watcher despawn self-joined the dying cell)")
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still addressable after death")
	}
}

// TestRespawnSameIDEachDeathIsIndependent: re-Spawning the same ActorID after a
// death is a fresh instance that, when it too dies, publishes its own embodiment
// down edge addressed by ActorID. Death is terminal per instance (no transparent
// respawn, no generation) — the substrate just produces one down per dying cell.
func TestRespawnSameIDEachDeathIsIndependent(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)

	rt.Spawn("a", actor.KindAgent, static(startPanicActor{}))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge for first instance")
	}

	rt.Spawn("a", actor.KindAgent, static(startPanicActor{}))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge for second instance")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.downs) != 2 {
		t.Fatalf("downs = %d, want 2", len(w.downs))
	}
	if w.downs[0] != "a" || w.downs[1] != "a" {
		t.Fatalf("downs = %+v, want both for actor a", w.downs)
	}
}

// ctxActor hands the reqCtx of each Receive to the test and (optionally) blocks
// on it / panics — so a test can observe the per-request scope the substrate
// derives.
type ctxActor struct {
	gotCtx chan context.Context
	block  bool   // block on reqCtx.Done() before returning
	panicN string // if env.ID == panicN, launch a downstream ctx-waiter then panic
	downCh chan struct{}
}

func (a *ctxActor) Receive(ctx context.Context, env *message.Envelope) error {
	if a.gotCtx != nil {
		a.gotCtx <- ctx
	}
	if a.panicN != "" && string(env.ID) == a.panicN {
		// A downstream goroutine the dead instance spawned, holding the reqCtx. The
		// death-path cell cancel must cascade into it so it unwinds (no leak).
		go func() {
			<-ctx.Done()
			close(a.downCh)
		}()
		panic("boom")
	}
	if a.block {
		<-ctx.Done()
	}
	return nil
}

// TestCellDeathCancelsInFlightReqCtx: when a cell dies (panic), the death path
// cancels the cell ctx (c.ctx), cascading into any downstream goroutine a Receive
// spawned holding that ctx so it unwinds (the actor-scope of cancel(scope) — no
// leaked work behind a corpse). 期10 S5 retired the per-request reqCtx; Receive
// now runs under c.ctx directly, and cell death cancelling c.ctx is what makes
// this cascade hold.
func TestCellDeathCancelsInFlightReqCtx(t *testing.T) {
	t.Parallel()
	a := &ctxActor{panicN: "boom-req", downCh: make(chan struct{})}
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", actor.KindAgent, static(a))
	mustDeliver(t, rt, "a", env("boom-req"))

	select {
	case <-a.downCh:
	case <-time.After(2 * time.Second):
		t.Fatal("dead cell's in-flight reqCtx was not cancelled — downstream goroutine leaked")
	}
}

// cancelHookActor implements RequestCanceller: cell.cancelRequest must hand
// the id off to it in one hop instead of firing the built-in reqCtx.
type cancelHookActor struct {
	calls chan message.ID
}

func (a *cancelHookActor) Receive(ctx context.Context, env *message.Envelope) error { return nil }
func (a *cancelHookActor) CancelRequest(id message.ID)                              { a.calls <- id }

// TestCancelRequestOccupantHook: an occupant implementing RequestCanceller
// receives the one-hop handoff — dispatch (runtime) and disposition
// (occupant) are separate.
func TestCancelRequestOccupantHook(t *testing.T) {
	t.Parallel()
	a := &cancelHookActor{calls: make(chan message.ID, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", actor.KindAgent, static(a))

	rt.CancelRequest("a", message.ID("req-1"))
	select {
	case got := <-a.calls:
		if got != message.ID("req-1") {
			t.Fatalf("CancelRequest id = %q, want req-1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("occupant CancelRequest hook never invoked")
	}
}

// dyingActor implements DownReporter on top of recordActor's Receive/Start/
// Stop — the occupant's own exit-code signal for the cell's Dying() select
// arm.
type dyingActor struct {
	*recordActor
	dying chan error
}

func newDyingActor() *dyingActor {
	return &dyingActor{recordActor: newRecordActor(), dying: make(chan error, 1)}
}

func (a *dyingActor) Dying() <-chan error { return a.dying }

// TestDownReporterQuietNoDownEdge: a nil value on Dying() (occupant "return
// nil") is quiet — the cell dies WITHOUT publishing the down edge.
func TestDownReporterQuietNoDownEdge(t *testing.T) {
	t.Parallel()
	a := newDyingActor()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	rt.Spawn("a", actor.KindAgent, static(a))

	a.dying <- nil
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := rt.Stat("a"); !ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cell never died on a quiet Dying() signal")
		case <-time.After(2 * time.Millisecond):
		}
	}
	select {
	case <-w.notify:
		t.Fatal("quiet death (nil) must not publish a down edge")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestDownReporterLoudPublishesDownEdge: a non-nil error on Dying() (occupant
// "return err") is loud — the cell publishes the down edge (author#3).
func TestDownReporterLoudPublishesDownEdge(t *testing.T) {
	t.Parallel()
	a := newDyingActor()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	rt.Spawn("a", actor.KindAgent, static(a))

	a.dying <- errors.New("boom")
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge after a loud Dying() signal")
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still addressable after loud death")
	}
}

// TestDownReporterStoppingOverridesLoud: arbitration ②>① — a stopping
// position (external Despawn in flight) forces quiet even when a loud
// Dying() error is queued, so a graceful shutdown never misfires
// receiver_unavailable.
func TestDownReporterStoppingOverridesLoud(t *testing.T) {
	t.Parallel()
	a := newDyingActor()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	inc := rt.Spawn("a", actor.KindAgent, static(a))

	a.dying <- errors.New("boom") // queued before the stopping position — races ctx.Done()
	rt.Despawn(inc)               // marks stopping (c.closed) before cancelling ctx

	select {
	case <-w.notify:
		t.Fatal("stopping position must force quiet, even with a loud Dying() error queued")
	case <-time.After(200 * time.Millisecond):
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still addressable after Despawn")
	}
}

// TestCellTeardownDoesNotWaitInFlight is the G0 inversion of the old
// "teardown waits" contract (DoD①: 卡死不陪葬). Despawn must return in
// O(judge-dead) WITHOUT joining a stuck in-flight Receive — the corpse is enrolled
// on the zombie ledger and its escort does the bounded join off-path. Once the
// Receive is released the goroutine exits and self-reaps (account⇔residue: the
// ledger clears).
func TestCellTeardownDoesNotWaitInFlight(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	finished := atomic.Bool{}
	a := newRecordActor()
	a.receive = func() {
		<-release
		finished.Store(true)
	}
	rt, _ := New(Config{Parent: context.Background(), ZombieGrace: 2 * time.Second})
	inc := rt.Spawn("a", actor.KindAgent, static(a))
	mustDeliver(t, rt, "a", env("x"))
	time.Sleep(10 * time.Millisecond) // ensure Receive is in-flight

	start := time.Now()
	rt.Despawn(inc) // must NOT block on the in-flight Receive
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Despawn blocked %v on an in-flight Receive — teardown must be O(judge-dead)", elapsed)
	}
	if finished.Load() {
		t.Fatal("in-flight Receive completed before release — test did not exercise the blocked path")
	}
	// The corpse is enrolled while its goroutine is still stuck in Receive.
	if got := len(rt.Zombies()); got != 1 {
		t.Fatalf("zombie ledger = %d entries, want 1 (the stuck corpse)", got)
	}
	// Release it: the goroutine exits and self-reaps, clearing the ledger.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for len(rt.Zombies()) != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := len(rt.Zombies()); got != 0 {
		t.Fatalf("zombie ledger = %d after release, want 0 (self-reap)", got)
	}
	if !finished.Load() {
		t.Fatal("released Receive never completed")
	}
	if rt.LeakedTotal() != 0 {
		t.Fatalf("LeakedTotal = %d, want 0 (the corpse exited within grace)", rt.LeakedTotal())
	}
}
