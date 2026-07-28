package actorhost

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// HostSupervisor.OnObs is the one place in the whole observation chain that
// decides whether a Unit's push is still worth forwarding. Everything upstream
// (the actor's ActorContext.PublishObs, Unit.publishObs) knows only "this
// incarnation says X"; everything downstream (HostEventSink consumers, the
// daemon's outbound slots) assumes what it receives is the CURRENT body's word.
// The filter here — exact Unit pointer AND exact Incarnation, both matched
// against the published body — is what makes that assumption true. Nothing
// tested it before this file.

// obsForwardEvent is the whole forwarded tuple. hostEventProbe (host_test.go)
// deliberately drops kind/value; a forwarding contract has to assert the
// payload arrives unmangled, so this file carries its own sink.
type obsForwardEvent struct {
	id    actor.ActorID
	key   AttemptKey
	self  actorrt.Incarnation
	kind  actorrt.ObsKind
	value actorrt.ObsValue
}

type obsForwardProbe struct {
	mu       sync.Mutex
	forwards []obsForwardEvent
	exits    int
}

func (p *obsForwardProbe) OnBodyExited(actor.ActorID, AttemptKey, actorrt.Incarnation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exits++
}

func (p *obsForwardProbe) OnBodyObs(
	id actor.ActorID,
	key AttemptKey,
	self actorrt.Incarnation,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.forwards = append(p.forwards, obsForwardEvent{
		id: id, key: key, self: self, kind: kind, value: value,
	})
}

func (p *obsForwardProbe) snapshot() []obsForwardEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]obsForwardEvent(nil), p.forwards...)
}

func (p *obsForwardProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.forwards)
}

// obsPublishingActor hands its ActorContext back to the test so the test can
// drive the REAL production push path (ActorContext.PublishObs → Unit.publishObs
// → sink.OnObs) instead of only calling OnObs directly.
type obsPublishingActor struct {
	ready chan actorrt.ActorContext
	once  sync.Once
}

func newObsPublishingActor() *obsPublishingActor {
	return &obsPublishingActor{ready: make(chan actorrt.ActorContext, 1)}
}

func (a *obsPublishingActor) Start(_ context.Context, self actorrt.ActorContext) error {
	a.once.Do(func() { a.ready <- self })
	return nil
}

func (*obsPublishingActor) Receive(context.Context, *message.Envelope) error { return nil }

func (a *obsPublishingActor) context(t *testing.T) actorrt.ActorContext {
	t.Helper()
	select {
	case ctx := <-a.ready:
		return ctx
	case <-time.After(3 * time.Second):
		t.Fatal("actor Start never ran")
		return nil
	}
}

// TestBodyObsFromTheCurrentIncarnationIsForwardedVerbatim walks the whole live
// path: a started body pushes, and the Host's sink sees that push carried with
// the body's own coordinate (its ActorID, its AttemptKey, its exact
// Incarnation) and its payload untouched. This is the positive half the filter
// exists to protect — a filter that dropped everything would also "never
// forward a stale observation".
func TestBodyObsFromTheCurrentIncarnationIsForwardedVerbatim(t *testing.T) {
	t.Parallel()
	probe := &obsForwardProbe{}
	impl := newObsPublishingActor()
	inputs := make(chan BodyBuildInput, 1)
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		Events:       probe,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			return impl
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:obs-live")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	input := <-inputs
	eventually(t, input.Current.IsCurrent)

	body := impl.context(t)
	body.PublishObs(actorrt.ObsKind("device_presence"), actorrt.ObsValue(`{"online":true}`))
	eventually(t, func() bool { return probe.count() == 1 })

	got := probe.snapshot()[0]
	if got.id != id || got.key != key {
		t.Fatalf("forwarded coordinate = (%q,%q), want the publishing body's own (%q,%q)",
			got.id, got.key, id, key)
	}
	if got.self != input.Self {
		t.Fatal("forwarded Incarnation is not the exact one that published")
	}
	if got.kind != "device_presence" || string(got.value) != `{"online":true}` {
		t.Fatalf("forwarded payload = (%q,%q), want it verbatim", got.kind, got.value)
	}
}

// TestBodyObsFromAReplacedPredecessorIsDropped is the judgement point itself.
// G1 runs, G2 replaces it, and G1's push — carrying G1's genuine Unit and
// Incarnation — must not reach the sink. Without this filter the consumer would
// receive a G1-authored observation stamped with nothing that distinguishes it
// from G2's, i.e. the channel's view of "what this actor reports right now"
// would silently regress to a dead incarnation's last word.
//
// The event is handed to OnObs directly on purpose: the replaced Unit is being
// stopped, so its own publishObs would short-circuit on !IsAlive() and the
// filter under test would never be reached. The coordinates are real (captured
// from G1's own build), so what is exercised is exactly the production
// comparison, not a fabricated shape.
func TestBodyObsFromAReplacedPredecessorIsDropped(t *testing.T) {
	t.Parallel()
	probe := &obsForwardProbe{}
	inputs := make(chan BodyBuildInput, 2)
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		Events:       probe,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:obs-predecessor")
	g1 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	first := <-inputs
	eventually(t, first.Current.IsCurrent)
	snapshot, ok := host.Inspect(id)
	if !ok || snapshot.Unit == nil {
		t.Fatal("G1 never published")
	}
	g1Unit := snapshot.Unit

	g2 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	second := <-inputs
	eventually(t, second.Current.IsCurrent)
	if first.Current.IsCurrent() {
		t.Fatal("G1 still reports current after G2 published")
	}

	host.OnObs(actorrt.UnitObsEvent{
		Unit:  g1Unit,
		Self:  first.Self,
		Kind:  actorrt.ObsKind("device_presence"),
		Value: actorrt.ObsValue(`{"online":false}`),
	})
	if n := probe.count(); n != 0 {
		t.Fatalf("replaced predecessor's observation was forwarded (%d events): %+v",
			n, probe.snapshot())
	}

	// The successor's own push still lands, so the drop above is a judgement
	// about WHICH incarnation spoke, not a jammed forwarding path.
	live, ok := host.Inspect(id)
	if !ok || live.Unit == nil {
		t.Fatal("G2 vanished")
	}
	host.OnObs(actorrt.UnitObsEvent{
		Unit:  live.Unit,
		Self:  second.Self,
		Kind:  actorrt.ObsKind("device_presence"),
		Value: actorrt.ObsValue(`{"online":true}`),
	})
	forwarded := probe.snapshot()
	if len(forwarded) != 1 || forwarded[0].key != g2 || forwarded[0].self != second.Self {
		t.Fatalf("successor observation = %+v, want exactly G2's", forwarded)
	}
}

// TestBodyObsFromARetiredBodyWithNoHostRowIsDropped covers the other end of the
// same window: not "someone else is current" but "nobody is". After the desired
// entry is withdrawn the Host row is collected entirely, and a straggler
// observation from the body being torn down must find no route to the sink
// (and must not fault on the missing row).
func TestBodyObsFromARetiredBodyWithNoHostRowIsDropped(t *testing.T) {
	t.Parallel()
	probe := &obsForwardProbe{}
	inputs := make(chan BodyBuildInput, 1)
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		Events:       probe,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:obs-retired")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	input := <-inputs
	eventually(t, input.Current.IsCurrent)
	snapshot, _ := host.Inspect(id)
	unit := snapshot.Unit

	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, present := host.Inspect(id)
		return !present
	})

	host.OnObs(actorrt.UnitObsEvent{
		Unit:  unit,
		Self:  input.Self,
		Kind:  actorrt.ObsKind("device_presence"),
		Value: actorrt.ObsValue(`{"online":false}`),
	})
	if n := probe.count(); n != 0 {
		t.Fatalf("observation from a body with no host row was forwarded: %+v", probe.snapshot())
	}
}

// TestBodyObsWithoutAnEventSinkIsANoOp pins the nil-Events assembly (every Host
// built without a consumer — the server domain's own tests, any embedding that
// only wants lifecycle) as a legitimate configuration rather than a latent nil
// dereference on the first push a body makes.
func TestBodyObsWithoutAnEventSinkIsANoOp(t *testing.T) {
	t.Parallel()
	impl := newObsPublishingActor()
	inputs := make(chan BodyBuildInput, 1)
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			return impl
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:obs-no-sink")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	input := <-inputs
	eventually(t, input.Current.IsCurrent)

	impl.context(t).PublishObs(actorrt.ObsKind("device_presence"), actorrt.ObsValue(`{"online":true}`))

	// The body must still be alive and current afterwards: a swallowed panic
	// inside the push would have taken the Unit down instead.
	if !input.Current.IsCurrent() {
		t.Fatal("publishing with no event sink took the body down")
	}
	snapshot, ok := host.Inspect(id)
	if !ok || snapshot.Unit == nil || !snapshot.Unit.IsAlive() {
		t.Fatal("body did not survive a push into a sink-less Host")
	}
}
