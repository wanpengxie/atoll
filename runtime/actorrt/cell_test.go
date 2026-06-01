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

// recordActor records the order and concurrency of Receive invocations
// WITHOUT any internal lock on the per-message fields — proving the
// substrate serializes for it. Lifecycle flags (started/stopped) are
// surfaced via channels because they are read from the test goroutine.
type recordActor struct {
	// no mutex on purpose: if the substrate is serial, these are race-free
	// (only ever touched on the cell goroutine).
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

func TestCellSerialDelivery(t *testing.T) {
	t.Parallel()
	done := make(chan struct{}, 100)
	a := newRecordActor()
	a.receive = func() {
		// Hold briefly so any concurrency would be observed by maxParallel.
		time.Sleep(time.Millisecond)
		done <- struct{}{}
	}
	rt := New(Config{Parent: context.Background(), Mailbox: 200})
	rt.Spawn("a", a)

	const n = 50
	for i := 0; i < n; i++ {
		if err := rt.Deliver(context.Background(), []actor.ActorID{"a"}, env(string(rune('A'+i%26)))); err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		<-done
	}
	// Stop establishes a happens-before edge (cell goroutine exit) so the
	// test goroutine can read the per-message fields race-free.
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
	rt := New(Config{Parent: context.Background()})
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
	if rt.Has("a") {
		t.Fatal("cell still present after Despawn")
	}
}

func TestCellMailboxFull(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	a := newRecordActor()
	a.receive = func() { <-block }
	rt := New(Config{Parent: context.Background(), Mailbox: 1})
	defer func() { close(block); rt.StopAll() }()
	rt.Spawn("a", a)

	// First message occupies the goroutine (blocked in Receive). Subsequent
	// ones fill the depth-1 mailbox then overflow.
	var gotFull atomic.Bool
	for i := 0; i < 50; i++ {
		err := rt.Deliver(context.Background(), []actor.ActorID{"a"}, env("x"))
		if err != nil {
			gotFull.Store(true)
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !gotFull.Load() {
		t.Fatal("expected ErrMailboxFull once mailbox saturated, never got it")
	}
}

type panicActor struct{}

func (panicActor) Receive(ctx context.Context, env *message.Envelope) error {
	panic("boom")
}

type recordingSupervisor struct {
	mu     sync.Mutex
	deaths []DeathSignal
	notify chan struct{}
}

func (s *recordingSupervisor) OnDeath(ctx context.Context, sig DeathSignal) {
	s.mu.Lock()
	s.deaths = append(s.deaths, sig)
	s.mu.Unlock()
	if s.notify != nil {
		s.notify <- struct{}{}
	}
}

func TestCellPanicSurfacesDeathSignal(t *testing.T) {
	t.Parallel()
	sup := &recordingSupervisor{notify: make(chan struct{}, 1)}
	rt := New(Config{Parent: context.Background(), Supervisor: sup})
	rt.Spawn("a", panicActor{})
	if err := rt.Deliver(context.Background(), []actor.ActorID{"a"}, env("x")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	select {
	case <-sup.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no death signal after actor panic")
	}
	sup.mu.Lock()
	defer sup.mu.Unlock()
	if len(sup.deaths) != 1 || sup.deaths[0].Actor != "a" {
		t.Fatalf("deaths = %+v, want one for actor a", sup.deaths)
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
	rt := New(Config{Parent: context.Background()})
	rt.Spawn("a", a)
	if err := rt.Deliver(context.Background(), []actor.ActorID{"a"}, env("x")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
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
