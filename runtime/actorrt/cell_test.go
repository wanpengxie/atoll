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
	res, err := rt.Deliver(context.Background(), []actor.ActorID{id}, e)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per[id].Outcome; got != Delivered {
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
	rt := New(Config{Parent: context.Background(), Mailbox: 200})
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

	var gotFull atomic.Bool
	for i := 0; i < 50; i++ {
		res, err := rt.Deliver(context.Background(), []actor.ActorID{"a"}, env("x"))
		if err != nil {
			t.Fatalf("deliver: %v", err)
		}
		if res.Per["a"].Outcome == MailboxFull {
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
	rt := New(Config{Parent: context.Background()})
	res, err := rt.Deliver(context.Background(), []actor.ActorID{"ghost"}, env("x"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per["ghost"].Outcome; got != NotHosted {
		t.Fatalf("outcome = %v, want NotHosted", got)
	}
}

type panicActor struct{}

func (panicActor) Receive(ctx context.Context, env *message.Envelope) error { panic("boom") }

type startPanicActor struct{}

func (startPanicActor) Receive(ctx context.Context, env *message.Envelope) error { return nil }
func (startPanicActor) Start(ctx context.Context, self ActorContext) error       { panic("start boom") }

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
	mustDeliver(t, rt, "a", env("x"))
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
	if sup.deaths[0].Incarnation != 1 {
		t.Fatalf("incarnation = %d, want 1", sup.deaths[0].Incarnation)
	}
	// Self-eviction: the dead instance is unaddressable WITHOUT the supervisor
	// despawning it (OnDeath runs AFTER eviction). (B1)
	if rt.Has("a") {
		t.Fatal("dead cell still addressable — did not self-evict")
	}
}

// despawningSupervisor reproduces the DANGEROUS legacy pattern (despawn in
// OnDeath). It must NOT deadlock: the cell self-evicts before OnDeath, so the
// Despawn no-ops instead of self-joining the dying goroutine. (B1 regression)
type despawningSupervisor struct {
	rt     *Runtime
	notify chan struct{}
}

func (s *despawningSupervisor) OnDeath(ctx context.Context, sig DeathSignal) {
	s.rt.Despawn(sig.Actor)
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func TestPanicDeathWithDespawningSupervisorDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	sup := &despawningSupervisor{notify: make(chan struct{}, 1)}
	rt := New(Config{Parent: context.Background(), Supervisor: sup})
	sup.rt = rt
	rt.Spawn("a", startPanicActor{})
	select {
	case <-sup.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("death path deadlocked (supervisor despawn self-joined the dying cell)")
	}
	if rt.Has("a") {
		t.Fatal("cell still addressable after death")
	}
}

// TestIncarnationIncrementsAcrossRespawn: a respawn under the same ActorID is a
// distinct instance; the death signal names which generation died. (B2)
func TestIncarnationIncrementsAcrossRespawn(t *testing.T) {
	t.Parallel()
	sup := &recordingSupervisor{notify: make(chan struct{}, 1)}
	rt := New(Config{Parent: context.Background(), Supervisor: sup})

	rt.Spawn("a", startPanicActor{})
	select {
	case <-sup.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no death signal for incarnation 1")
	}

	rt.Spawn("a", startPanicActor{})
	select {
	case <-sup.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no death signal for incarnation 2")
	}

	sup.mu.Lock()
	defer sup.mu.Unlock()
	if len(sup.deaths) != 2 {
		t.Fatalf("deaths = %d, want 2", len(sup.deaths))
	}
	if sup.deaths[0].Incarnation != 1 || sup.deaths[1].Incarnation != 2 {
		t.Fatalf("incarnations = %d,%d, want 1,2", sup.deaths[0].Incarnation, sup.deaths[1].Incarnation)
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
