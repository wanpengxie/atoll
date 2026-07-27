package actorrt

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// panickingUnitSink is the hostile consumer: it records the event it was given
// and then panics, exactly as a faulty actorhost implementation would.
type panickingUnitSink struct {
	exited      chan ExitedEvent
	obs         chan UnitObsEvent
	panicExited bool
	panicObs    bool
}

func newPanickingUnitSink() *panickingUnitSink {
	return &panickingUnitSink{
		exited: make(chan ExitedEvent, 8),
		obs:    make(chan UnitObsEvent, 8),
	}
}

func (s *panickingUnitSink) OnExited(event ExitedEvent) {
	s.exited <- event
	if s.panicExited {
		panic("sink exited boom")
	}
}

func (s *panickingUnitSink) OnObs(event UnitObsEvent) {
	s.obs <- event
	if s.panicObs {
		panic("sink obs boom")
	}
}

// TestExitedSinkPanicDoesNotEscapeUnit pins that a consumer panic on the exit
// edge is absorbed at the emit boundary: the Unit still joins its organs, still
// runs the actor's Stop hook, and still closes Done.
func TestExitedSinkPanicDoesNotEscapeUnit(t *testing.T) {
	t.Parallel()

	logs := &lockedLogWriter{}
	sink := newPanickingUnitSink()
	sink.panicExited = true
	impl := newUnitProbeActor()
	u, _ := prepareProbe(t, "agent:sink-exit-panic", impl, sink, slog.New(slog.NewTextHandler(logs, nil)))
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	<-impl.started

	cause := errors.New("actor finished")
	impl.dying <- cause
	waitDone(t, u)

	select {
	case event := <-sink.exited:
		if !errors.Is(event.Cause, cause) {
			t.Fatalf("exit event = %#v, want the exact cause", event)
		}
	case <-time.After(cancelOrganWait):
		t.Fatal("sink never received the exit event")
	}
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls = %d, want 1 — the sink panic skipped cleanup", impl.stopped.Load())
	}
	if !strings.Contains(logs.String(), "actorrt.unit.exited_sink_panicked") {
		t.Fatalf("missing exited-sink panic log: %s", logs.String())
	}
}

// TestObsSinkPanicDoesNotEscapeUnit pins the same boundary on the observation
// lane: a consumer panic must not reach the actor goroutine, so the actor keeps
// serving work afterwards.
func TestObsSinkPanicDoesNotEscapeUnit(t *testing.T) {
	t.Parallel()

	logs := &lockedLogWriter{}
	sink := newPanickingUnitSink()
	sink.panicObs = true
	impl := newObsPublishActor()
	u, _ := prepareProbe(t, "agent:sink-obs-panic", impl, sink, slog.New(slog.NewTextHandler(logs, nil)))
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}

	for _, id := range []message.ID{"o1", "o2"} {
		if err := u.Deliver(&message.Envelope{ID: id}); err != nil {
			t.Fatalf("Deliver %s: %v", id, err)
		}
		select {
		case event := <-sink.obs:
			if string(event.Value) != string(id) {
				t.Fatalf("obs event = %#v, want value %s", event, id)
			}
		case <-time.After(cancelOrganWait):
			t.Fatalf("sink never received observation %s", id)
		}
		select {
		case <-impl.published:
		case <-time.After(cancelOrganWait):
			t.Fatalf("the actor goroutine did not survive the sink panic on %s", id)
		}
	}
	if !u.IsAlive() {
		t.Fatal("unit died from a consumer-side obs panic")
	}
	if !strings.Contains(logs.String(), "actorrt.unit.obs_sink_panicked") {
		t.Fatalf("missing obs-sink panic log: %s", logs.String())
	}

	u.Stop()
	waitDone(t, u)
	if impl.stopped.Load() != 1 {
		t.Fatalf("Stop calls = %d, want 1", impl.stopped.Load())
	}
}
