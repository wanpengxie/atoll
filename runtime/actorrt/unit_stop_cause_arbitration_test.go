package actorrt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// stopRaceProbe can be parked in Start or in Receive so a Stop request is
// guaranteed to be registered before the run loop produces any cause. This
// removes the need to race the two edges by timing.
type stopRaceProbe struct {
	startEntered chan struct{}
	startGate    chan struct{}
	startErr     error

	receiveEntered chan struct{}
	receiveGate    chan struct{}
	panicOnReceive bool

	dying   chan error
	stopped atomic.Int64
}

func newStopRaceProbe() *stopRaceProbe {
	return &stopRaceProbe{
		startEntered:   make(chan struct{}, 1),
		receiveEntered: make(chan struct{}, 1),
		dying:          make(chan error, 1),
	}
}

func (a *stopRaceProbe) Start(context.Context, ActorContext) error {
	select {
	case a.startEntered <- struct{}{}:
	default:
	}
	if a.startGate != nil {
		<-a.startGate
	}
	return a.startErr
}

func (a *stopRaceProbe) Receive(context.Context, *message.Envelope) error {
	select {
	case a.receiveEntered <- struct{}{}:
	default:
	}
	if a.receiveGate != nil {
		<-a.receiveGate
	}
	if a.panicOnReceive {
		panic("receive boom during stop")
	}
	return nil
}

func (a *stopRaceProbe) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}

func (a *stopRaceProbe) Dying() <-chan error { return a.dying }

func waitStopEntered(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(cancelOrganWait):
		t.Fatalf("actor never entered %s", what)
	}
}

func assertSingleQuietExit(t *testing.T, sink *unitSink, u *Unit) {
	t.Helper()
	select {
	case event := <-sink.exited:
		if event.Unit != u {
			t.Fatalf("exit event lost exact identity: %#v", event)
		}
		if event.Cause != nil {
			t.Fatalf("requested Stop reported cause %v, want nil", event.Cause)
		}
	case <-time.After(cancelOrganWait):
		t.Fatal("no exit event was published")
	}
	select {
	case event := <-sink.exited:
		t.Fatalf("duplicate exit event: %#v", event)
	default:
	}
}

// TestStopRequestClearsDyingCause pins the arbitration in finish(): once Stop
// was requested, the exit is a deliberate stop, so a Dying() report that lands
// in the same window must not be published as a fault cause.
func TestStopRequestClearsDyingCause(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := newStopRaceProbe()
	impl.receiveGate = make(chan struct{})
	u, _ := prepareProbe(t, "agent:stop-vs-dying", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "m1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	waitStopEntered(t, impl.receiveEntered, "Receive")

	// The occupant's own exit code is already queued when the stop is
	// requested; releasing Receive makes both select arms ready at once.
	impl.dying <- context.Canceled
	u.Stop()
	close(impl.receiveGate)
	waitDone(t, u)

	assertSingleQuietExit(t, sink, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop hook calls = %d, want 1", impl.stopped.Load())
	}
}

// TestStopRequestClearsReceivePanicCause pins the same arbitration for a panic
// raised after the stop was requested.
func TestStopRequestClearsReceivePanicCause(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := newStopRaceProbe()
	impl.receiveGate = make(chan struct{})
	impl.panicOnReceive = true
	u, _ := prepareProbe(t, "agent:stop-vs-panic", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "m1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	waitStopEntered(t, impl.receiveEntered, "Receive")

	u.Stop()
	close(impl.receiveGate)
	waitDone(t, u)

	assertSingleQuietExit(t, sink, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop hook calls = %d, want 1", impl.stopped.Load())
	}
}

// TestStopRequestClearsStartErrorCause pins the third input of the same race:
// a Start that fails only after the stop was requested is still a quiet stop.
func TestStopRequestClearsStartErrorCause(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := newStopRaceProbe()
	impl.startGate = make(chan struct{})
	impl.startErr = context.DeadlineExceeded
	u, _ := prepareProbe(t, "agent:stop-vs-start-error", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	waitStopEntered(t, impl.startEntered, "Start")

	u.Stop()
	close(impl.startGate)
	waitDone(t, u)

	assertSingleQuietExit(t, sink, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop hook calls = %d, want 1", impl.stopped.Load())
	}
}

// TestStopOfPreparedUnitPublishesNoExitEdge pins the other half of the same
// decision: a Unit that never started has no exit edge to publish, so a losing
// candidate's cleanup cannot be mistaken for a death.
func TestStopOfPreparedUnitPublishesNoExitEdge(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := newStopRaceProbe()
	u, _ := prepareProbe(t, "agent:stop-prepared", impl, sink, nil)

	u.Stop()
	waitDone(t, u)

	select {
	case event := <-sink.exited:
		t.Fatalf("never-started unit published an exit edge: %#v", event)
	case <-time.After(100 * time.Millisecond):
	}
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop hook calls = %d, want 1", impl.stopped.Load())
	}
}
