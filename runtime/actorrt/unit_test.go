package actorrt

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type unitSink struct {
	exited chan ExitedEvent
	obs    chan UnitObsEvent
}

func newUnitSink() *unitSink {
	return &unitSink{
		exited: make(chan ExitedEvent, 8),
		obs:    make(chan UnitObsEvent, 8),
	}
}
func (s *unitSink) OnExited(event ExitedEvent) { s.exited <- event }
func (s *unitSink) OnObs(event UnitObsEvent)   { s.obs <- event }

type unitProbeActor struct {
	started chan ActorContext
	recv    chan message.ID
	stopped atomic.Int64
	dying   chan error
}

func newUnitProbeActor() *unitProbeActor {
	return &unitProbeActor{
		started: make(chan ActorContext, 1),
		recv:    make(chan message.ID, 8),
		dying:   make(chan error, 1),
	}
}
func (a *unitProbeActor) Start(_ context.Context, self ActorContext) error {
	a.started <- self
	return nil
}
func (a *unitProbeActor) Receive(_ context.Context, env *message.Envelope) error {
	a.recv <- env.ID
	return nil
}
func (a *unitProbeActor) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}
func (a *unitProbeActor) Dying() <-chan error { return a.dying }

func prepareProbe(t *testing.T, id actor.ActorID, impl Actor, sink UnitEventSink, logger *slog.Logger) (*Unit, Incarnation) {
	t.Helper()
	var self Incarnation
	u, err := Prepare(UnitConfig{
		ActorID: id,
		Kind:    actor.KindAgent,
		Logger:  logger,
	}, func(got Incarnation) Actor {
		self = got
		return impl
	}, sink)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return u, self
}

func waitDone(t *testing.T, u *Unit) {
	t.Helper()
	select {
	case <-u.Done():
	case <-time.After(time.Second):
		t.Fatal("Unit.Done did not close")
	}
}

func TestUnitPrepareAllocatesExactSelfBeforeBuilder(t *testing.T) {
	t.Parallel()

	impl := newUnitProbeActor()
	u, self := prepareProbe(t, "agent:one", impl, nil, nil)
	if self.ID() != "agent:one" || self.unit != u {
		t.Fatalf("builder self = %#v, want exact Unit", self)
	}
	if u.Self() != self {
		t.Fatal("Unit.Self differs from builder identity")
	}
	if u.IsAlive() || u.State() != UnitPrepared {
		t.Fatalf("prepared unit is live/state=%v", u.State())
	}
	u.Stop()
	waitDone(t, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls = %d, want 1", impl.stopped.Load())
	}
}

func TestTwoUnitsWithSameActorIDAreIndependent(t *testing.T) {
	t.Parallel()

	a1, a2 := newUnitProbeActor(), newUnitProbeActor()
	s1, s2 := newUnitSink(), newUnitSink()
	u1, i1 := prepareProbe(t, "agent:same", a1, s1, nil)
	u2, i2 := prepareProbe(t, "agent:same", a2, s2, nil)
	if i1 == i2 || i1.unit == i2.unit {
		t.Fatal("same ActorID units share exact identity")
	}
	if err := u1.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u2.Start(); err != nil {
		t.Fatal(err)
	}
	<-a1.started
	<-a2.started

	u1.Stop()
	waitDone(t, u1)
	if !u2.IsAlive() {
		t.Fatal("stopping predecessor affected independent successor")
	}
	if err := u2.Deliver(&message.Envelope{ID: "m2"}); err != nil {
		t.Fatalf("u2 Deliver: %v", err)
	}
	select {
	case got := <-a2.recv:
		if got != "m2" {
			t.Fatalf("received %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("u2 did not receive")
	}
	event := <-s1.exited
	if event.Unit != u1 || event.Self != i1 {
		t.Fatal("exit event lost exact Unit identity")
	}
	select {
	case <-s2.exited:
		t.Fatal("u1 stop emitted u2 exit")
	default:
	}
	u2.Stop()
	waitDone(t, u2)
}

func TestUnitNaturalExitEmitsCauseOnce(t *testing.T) {
	t.Parallel()

	impl := newUnitProbeActor()
	sink := newUnitSink()
	u, self := prepareProbe(t, "agent:natural", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	<-impl.started
	want := errors.New("finished")
	impl.dying <- want
	waitDone(t, u)

	select {
	case event := <-sink.exited:
		if event.Unit != u || event.Self != self || !errors.Is(event.Cause, want) {
			t.Fatalf("event = %#v, want exact cause", event)
		}
	default:
		t.Fatal("missing exit event")
	}
	select {
	case event := <-sink.exited:
		t.Fatalf("duplicate exit event: %#v", event)
	default:
	}
}

type stopPanicActor struct{ stopped atomic.Int64 }

func (*stopPanicActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a *stopPanicActor) Stop(context.Context) error {
	a.stopped.Add(1)
	panic("boom")
}

func TestUnitStopPanicIsContainedAndDoneCloses(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	impl := &stopPanicActor{}
	u, _ := prepareProbe(t, "agent:panic-stop", impl, nil, logger)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	u.Stop()
	waitDone(t, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls = %d, want 1", impl.stopped.Load())
	}
	got := logs.String()
	if !strings.Contains(got, "actorrt.cell.stop_panicked") || !strings.Contains(got, "stack=") {
		t.Fatalf("panic log missing event/stack: %s", got)
	}
}

func TestPreparedLoserStopPanicUsesSameBoundary(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	impl := &stopPanicActor{}
	u, _ := prepareProbe(t, "agent:panic-loser", impl, nil, logger)
	u.Stop()
	waitDone(t, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls = %d, want 1", impl.stopped.Load())
	}
	if !strings.Contains(logs.String(), "actorrt.cell.stop_panicked") {
		t.Fatalf("missing prepared-loser panic log: %s", logs.String())
	}
}

type stopErrorActor struct{ err error }

func (*stopErrorActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a *stopErrorActor) Stop(context.Context) error                     { return a.err }

func TestUnitStopErrorIsLoggedAndDoneCloses(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	u, _ := prepareProbe(t, "agent:error-stop", &stopErrorActor{err: errors.New("release failed")}, nil, logger)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	u.Stop()
	waitDone(t, u)
	if !strings.Contains(logs.String(), "actorrt.cell.stop_abandoned") {
		t.Fatalf("missing stop error log: %s", logs.String())
	}
}

type blockingStopActor struct {
	entered chan struct{}
	release chan struct{}
}

func (*blockingStopActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a *blockingStopActor) Stop(context.Context) error {
	close(a.entered)
	<-a.release
	return nil
}

func TestUnitDoneWaitsForStopThatIgnoresContext(t *testing.T) {
	impl := &blockingStopActor{entered: make(chan struct{}), release: make(chan struct{})}
	u, _ := prepareProbe(t, "agent:block-stop", impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	u.Stop()
	select {
	case <-impl.entered:
	case <-time.After(time.Second):
		t.Fatal("Stop was not entered")
	}
	select {
	case <-u.Done():
		t.Fatal("Done closed while Stop remained blocked")
	case <-time.After(30 * time.Millisecond):
	}
	close(impl.release)
	waitDone(t, u)
}

func TestPrepareRejectsNilAndPanickingBuilder(t *testing.T) {
	t.Parallel()

	cfg := UnitConfig{ActorID: "agent:bad", Kind: actor.KindAgent}
	if _, err := Prepare(cfg, func(Incarnation) Actor { return nil }, nil); err == nil {
		t.Fatal("nil actor accepted")
	}
	if _, err := Prepare(cfg, func(Incarnation) Actor { panic("builder") }, nil); err == nil {
		t.Fatal("builder panic accepted")
	}
}
