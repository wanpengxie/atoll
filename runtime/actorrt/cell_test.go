package actorrt

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
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
	rt, _, _ := New(Config{Parent: context.Background(), Mailbox: 200})
	rt.Spawn("a", a)

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
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.Spawn("a", a)
	select {
	case <-a.startedCh:
	case <-time.After(time.Second):
		t.Fatal("Start never ran")
	}
	rt.Despawn("a")
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
	rt, _, _ := New(Config{Parent: context.Background(), Mailbox: 1})
	defer func() { close(block); rt.StopAll() }()
	rt.Spawn("a", a)

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
	rt, _, _ := New(Config{Parent: context.Background()})
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

// recordingWatcher is the consumer end of the obs presence-push channel: it
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

func TestCellPanicPublishesPresenceDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w) // register BEFORE spawn — no edge missed
	rt.Spawn("a", panicActor{})
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no presence-down edge after actor panic")
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
// Despawn no-ops instead of self-joining the dying goroutine. (B1 regression)
type despawningWatcher struct {
	rt     *Runtime
	notify chan struct{}
}

func (w *despawningWatcher) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	w.rt.Despawn(id)
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func TestPanicDeathWithDespawningWatcherDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	w := &despawningWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	w.rt = rt
	rt.WatchPresence(w)
	rt.Spawn("a", startPanicActor{})
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
// death is a fresh instance that, when it too dies, publishes its own presence
// down edge addressed by ActorID. Death is terminal per instance (no transparent
// respawn, no generation) — the substrate just produces one down per dying cell.
func TestRespawnSameIDEachDeathIsIndependent(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)

	rt.Spawn("a", startPanicActor{})
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no presence-down edge for first instance")
	}

	rt.Spawn("a", startPanicActor{})
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no presence-down edge for second instance")
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

func TestCellTeardownWaitsInFlight(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	finished := atomic.Bool{}
	a := newRecordActor()
	a.receive = func() {
		<-release
		finished.Store(true)
	}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.Spawn("a", a)
	mustDeliver(t, rt, "a", env("x"))
	time.Sleep(10 * time.Millisecond) // ensure Receive is in-flight
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	rt.Despawn("a") // must block until in-flight Receive returns
	if !finished.Load() {
		t.Fatal("Despawn returned before in-flight Receive completed")
	}
}
