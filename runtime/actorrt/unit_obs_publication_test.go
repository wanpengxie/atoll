package actorrt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// obsPublishActor publishes one observation per envelope from the Unit
// goroutine — the production shape of the PUSH producer end.
type obsPublishActor struct {
	self      ActorContext
	published chan struct{}
	stopped   atomic.Int64
}

func newObsPublishActor() *obsPublishActor {
	return &obsPublishActor{published: make(chan struct{}, 8)}
}

func (a *obsPublishActor) Start(_ context.Context, self ActorContext) error {
	a.self = self
	return nil
}

func (a *obsPublishActor) Receive(_ context.Context, env *message.Envelope) error {
	a.self.PublishObs(ObsKind("probe"), ObsValue(env.ID))
	a.published <- struct{}{}
	return nil
}

func (a *obsPublishActor) Stop(context.Context) error {
	a.stopped.Add(1)
	return nil
}

func waitObs(t *testing.T, sink *unitSink) UnitObsEvent {
	t.Helper()
	select {
	case event := <-sink.obs:
		return event
	case <-time.After(cancelOrganWait):
		t.Fatal("no observation reached the sink")
		return UnitObsEvent{}
	}
}

// TestPublishObsReachesSinkWithExactIncarnation pins the whole observation
// egress: ActorContext.PublishObs → Unit.publishObs → sink.OnObs, carrying this
// exact Unit and Incarnation so a consumer can reject a predecessor's obs.
func TestPublishObsReachesSinkWithExactIncarnation(t *testing.T) {
	t.Parallel()

	sink := newUnitSink()
	impl := newObsPublishActor()
	u, self := prepareProbe(t, "agent:obs-egress", impl, sink, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "o1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	event := waitObs(t, sink)
	if event.Unit != u || event.Self != self {
		t.Fatalf("obs event lost exact identity: %#v", event)
	}
	if event.Kind != ObsKind("probe") || string(event.Value) != "o1" {
		t.Fatalf("obs event = %#v, want probe/o1", event)
	}

	// Two same-ActorID Units must not share an observation stream.
	otherSink := newUnitSink()
	otherImpl := newObsPublishActor()
	other, otherSelf := prepareProbe(t, "agent:obs-egress", otherImpl, otherSink, nil)
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	if err := other.Deliver(&message.Envelope{ID: "o2"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	otherEvent := waitObs(t, otherSink)
	if otherEvent.Unit != other || otherEvent.Self != otherSelf || otherEvent.Self == self {
		t.Fatalf("successor obs event = %#v, want its own incarnation", otherEvent)
	}
	select {
	case stray := <-sink.obs:
		t.Fatalf("predecessor sink received a successor observation: %#v", stray)
	default:
	}
	other.Stop()
	waitDone(t, other)

	u.Stop()
	waitDone(t, u)

	// A dead Unit publishes nothing: the liveness gate is the observation's
	// only validity window.
	impl.self.PublishObs(ObsKind("probe"), ObsValue("after-stop"))
	select {
	case stray := <-sink.obs:
		t.Fatalf("observation published after the unit stopped: %#v", stray)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestPublishObsWithoutSinkIsNoOp pins that an unadopted Unit (no event owner)
// simply drops observations instead of failing.
func TestPublishObsWithoutSinkIsNoOp(t *testing.T) {
	t.Parallel()

	impl := newObsPublishActor()
	u, _ := prepareProbe(t, "agent:obs-no-sink", impl, nil, nil)
	if err := u.Start(); err != nil {
		t.Fatal(err)
	}
	if err := u.Deliver(&message.Envelope{ID: "o1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	select {
	case <-impl.published:
	case <-time.After(cancelOrganWait):
		t.Fatal("actor did not complete a publish without a sink")
	}
	u.Stop()
	waitDone(t, u)
}
