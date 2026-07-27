package actorrt

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// runReceivePanicActor panics on its first envelope and counts Stop calls, so
// the test can prove the run() recover still reaches the cleanup path.
type runReceivePanicActor struct {
	received atomic.Int64
	stopped  atomic.Int64
}

func (a *runReceivePanicActor) Receive(context.Context, *message.Envelope) error {
	a.received.Add(1)
	panic("receive boom")
}

func (a *runReceivePanicActor) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}

// runStartPanicActor panics inside the Start lifecycle hook, before any
// envelope can be observed.
type runStartPanicActor struct {
	received atomic.Int64
	stopped  atomic.Int64
}

func (*runStartPanicActor) Start(context.Context, ActorContext) error { panic("start boom") }
func (a *runStartPanicActor) Receive(context.Context, *message.Envelope) error {
	a.received.Add(1)
	return nil
}
func (a *runStartPanicActor) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}

// runStartErrorActor fails Start deterministically with an identifiable
// sentinel.
type runStartErrorActor struct {
	err     error
	stopped atomic.Int64
}

func (a *runStartErrorActor) Start(context.Context, ActorContext) error { return a.err }
func (*runStartErrorActor) Receive(context.Context, *message.Envelope) error {
	return nil
}
func (a *runStartErrorActor) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}

func waitExited(t *testing.T, sink *unitSink) ExitedEvent {
	t.Helper()
	select {
	case event := <-sink.exited:
		return event
	case <-time.After(cancelOrganWait):
		t.Fatal("no exit event was published")
		return ExitedEvent{}
	}
}

// TestReceivePanicBecomesExitCauseAndSparesSiblingUnits pins the run() top-level
// recover for the work lane: the panic is converted into this exact Unit's exit
// cause, the Stop hook still runs, and an independent Unit keeps working.
func TestReceivePanicBecomesExitCauseAndSparesSiblingUnits(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := &runReceivePanicActor{}
	u, self := prepareProbe(t, "agent:panic-receive", impl, sink, nil)

	sibling := newUnitProbeActor()
	healthy, _ := prepareProbe(t, "agent:panic-receive-sibling", sibling, newUnitSink(), nil)
	if err := healthy.Start(); err != nil {
		t.Fatal(err)
	}
	<-sibling.started

	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "m1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	waitDone(t, u)

	event := waitExited(t, sink)
	if event.Unit != u || event.Self != self {
		t.Fatalf("exit event lost exact identity: %#v", event)
	}
	if event.Cause == nil || !strings.Contains(event.Cause.Error(), "panicked") ||
		!strings.Contains(event.Cause.Error(), "receive boom") {
		t.Fatalf("exit cause = %v, want the recovered Receive panic", event.Cause)
	}
	if u.IsAlive() || u.State() != UnitDone {
		t.Fatalf("panicked unit state = %v, alive = %v", u.State(), u.IsAlive())
	}
	if err := u.Deliver(&message.Envelope{ID: "m2"}); !errors.Is(err, ErrUnitStopped) {
		t.Fatalf("post-panic Deliver error = %v", err)
	}
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls after panic = %d, want 1", impl.stopped.Load())
	}

	if err := healthy.Deliver(&message.Envelope{ID: "s1"}); err != nil {
		t.Fatalf("sibling Deliver: %v", err)
	}
	select {
	case got := <-sibling.recv:
		if got != "s1" {
			t.Fatalf("sibling received %q", got)
		}
	case <-time.After(cancelOrganWait):
		t.Fatal("sibling unit stopped working after another unit panicked")
	}
	healthy.Stop()
	waitDone(t, healthy)
}

// TestStartPanicBecomesExitCause pins the same recover for the Start phase: the
// Unit never admits work and reports the panic as its exit cause.
func TestStartPanicBecomesExitCause(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := &runStartPanicActor{}
	u, self := prepareProbe(t, "agent:panic-start", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, u)

	event := waitExited(t, sink)
	if event.Unit != u || event.Self != self {
		t.Fatalf("exit event lost exact identity: %#v", event)
	}
	if event.Cause == nil || !strings.Contains(event.Cause.Error(), "panicked") ||
		!strings.Contains(event.Cause.Error(), "start boom") {
		t.Fatalf("exit cause = %v, want the recovered Start panic", event.Cause)
	}
	if impl.received.Load() != 0 {
		t.Fatalf("Receive ran %d times after a Start panic", impl.received.Load())
	}
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls after Start panic = %d, want 1", impl.stopped.Load())
	}
}

// TestStartErrorBecomesWrappedExitCause pins the non-panic sibling of the same
// edge: a Start error is wrapped, not swallowed, and still ends the Unit.
func TestStartErrorBecomesWrappedExitCause(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("resource unavailable")
	sink := newUnitSink()
	impl := &runStartErrorActor{err: sentinel}
	u, _ := prepareProbe(t, "agent:start-error", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, u)

	event := waitExited(t, sink)
	if !errors.Is(event.Cause, sentinel) {
		t.Fatalf("exit cause = %v, want wrapped %v", event.Cause, sentinel)
	}
	if !strings.Contains(event.Cause.Error(), "Start failed") {
		t.Fatalf("exit cause lost its Start phase label: %v", event.Cause)
	}
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls after Start error = %d, want 1", impl.stopped.Load())
	}
}
