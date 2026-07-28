package actorhost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// inspectTransitional reports the in-flight build/retire occupancy for one
// actor — a test-only probe (same locking discipline as Inspect). Production
// Snapshot deliberately carries only converged coordinates.
func (h *HostSupervisor) inspectTransitional(id actor.ActorID) (building bool, retiring int) {
	unlock := h.spans.lock(id)
	defer unlock()
	h.mu.RLock()
	defer h.mu.RUnlock()
	state := h.states[id]
	if state == nil {
		return false, 0
	}
	return state.build != nil, len(state.retiring)
}

type hostTestActor struct {
	dying chan error
	recv  chan message.ID
}

func newHostTestActor() *hostTestActor {
	return &hostTestActor{
		dying: make(chan error, 1),
		recv:  make(chan message.ID, 8),
	}
}

func (a *hostTestActor) Receive(_ context.Context, env *message.Envelope) error {
	if env != nil {
		a.recv <- env.ID
	}
	return nil
}

func (a *hostTestActor) Dying() <-chan error { return a.dying }

type hostEventProbe struct {
	exits chan eventExit
	obs   chan eventObs
}

type eventExit struct {
	id    actor.ActorID
	key   AttemptKey
	self  actorrt.Incarnation
	cause error
}

type eventObs struct {
	id   actor.ActorID
	key  AttemptKey
	self actorrt.Incarnation
}

func newHostEventProbe() *hostEventProbe {
	return &hostEventProbe{
		exits: make(chan eventExit, 16),
		obs:   make(chan eventObs, 16),
	}
}

func (p *hostEventProbe) OnBodyExited(id actor.ActorID, key AttemptKey, self actorrt.Incarnation, cause error) {
	p.exits <- eventExit{id: id, key: key, self: self, cause: cause}
}

func (p *hostEventProbe) OnBodyObs(id actor.ActorID, key AttemptKey, self actorrt.Incarnation, _ actorrt.ObsKind, _ actorrt.ObsValue) {
	p.obs <- eventObs{id: id, key: key, self: self}
}

type testBinding struct {
	closeOnce sync.Once
	closed    chan struct{}
	done      chan struct{}
	deliver   chan message.ID
	cancel    chan message.ID
	block     chan struct{}
	calls     atomic.Int64
}

type nonComparableBinding struct {
	payload []byte
	done    <-chan struct{}
}

func (nonComparableBinding) Deliver(*message.Envelope) error { return nil }
func (nonComparableBinding) CancelRequest(message.ID)        {}
func (nonComparableBinding) Close() error                    { return nil }
func (b nonComparableBinding) Done() <-chan struct{}         { return b.done }

func newTestBinding() *testBinding {
	return &testBinding{
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
		deliver: make(chan message.ID, 16),
		cancel:  make(chan message.ID, 16),
	}
}

func (b *testBinding) Deliver(env *message.Envelope) error {
	b.calls.Add(1)
	if b.block != nil {
		<-b.block
	}
	select {
	case <-b.closed:
		return errors.New("closed")
	default:
	}
	b.deliver <- env.ID
	return nil
}

func (b *testBinding) CancelRequest(id message.ID) { b.cancel <- id }
func (b *testBinding) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}
func (b *testBinding) Done() <-chan struct{} { return b.done }
func (b *testBinding) finish() {
	_ = b.Close()
	select {
	case <-b.done:
	default:
		close(b.done)
	}
}

func exactTestBinding(t *testing.T, resource BindingResource) Binding {
	t.Helper()
	binding, err := NewBinding(resource)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func testAttempt(t *testing.T) AttemptKey {
	t.Helper()
	key, err := NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func bodyDesiredFor(t *testing.T, id actor.ActorID, key AttemptKey) BodyDesired {
	t.Helper()
	return BodyDesired{
		ActorID:    id,
		AttemptKey: key,
		ExecutionSpec: ExecutionSpec{
			Kind:   actor.KindAgent,
			Class:  "test",
			Config: []byte(`{"b":2,"a":1}`),
		},
	}
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not converge")
}

func closeHost(t *testing.T, host *HostSupervisor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAttemptKeyCanonicalUUIDv7AndWholeValueOrder(t *testing.T) {
	t.Parallel()
	left := testAttempt(t)
	right := testAttempt(t)
	if _, err := ParseAttemptKey(string(left)); err != nil {
		t.Fatalf("ParseAttemptKey: %v", err)
	}
	if _, err := ParseAttemptKey("00000000-0000-4000-8000-000000000000"); !errors.Is(err, ErrInvalidAttemptKey) {
		t.Fatalf("non-v7 error = %v", err)
	}
	got, err := compareAttemptKeys(left, right)
	if err != nil {
		t.Fatal(err)
	}
	want := 1
	if string(left) < string(right) {
		want = -1
	} else if left == right {
		want = 0
	}
	if got != want {
		t.Fatalf("comparison = %d, want %d", got, want)
	}
}

func TestFullDesiredRejectRetainsLastKnownGood(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:lkg")
	g1 := testAttempt(t)
	initial := CarrierDesired{ActorID: id, AttemptKey: g1, PeerDomain: "daemon-1"}
	if err := host.AcceptFullDesired([]Desired{initial}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := host.Inspect(id)
	if !ok || snapshot.Desired.(CarrierDesired).AttemptKey != g1 {
		t.Fatalf("initial snapshot = %#v", snapshot)
	}

	drift := CarrierDesired{ActorID: id, AttemptKey: g1, PeerDomain: "daemon-2"}
	if err := host.AcceptFullDesired([]Desired{drift}); !errors.Is(err, ErrSameAttemptDrift) {
		t.Fatalf("drift error = %v", err)
	}
	snapshot, ok = host.Inspect(id)
	if !ok || snapshot.Desired.(CarrierDesired).PeerDomain != "daemon-1" {
		t.Fatalf("bad snapshot changed LKG: %#v", snapshot)
	}

	fresh := CarrierDesired{ActorID: id, AttemptKey: testAttempt(t), PeerDomain: "daemon-2"}
	invalid := CarrierDesired{ActorID: "agent:bad", AttemptKey: "not-a-key", PeerDomain: "daemon-2"}
	if err := host.AcceptFullDesired([]Desired{fresh, invalid}); !errors.Is(err, ErrInvalidAttemptKey) {
		t.Fatalf("invalid error = %v", err)
	}
	snapshot, _ = host.Inspect(id)
	if snapshot.Desired.(CarrierDesired).AttemptKey != g1 {
		t.Fatalf("partial bad snapshot changed LKG: %#v", snapshot)
	}
}

func TestBodyBuildReceivesExactSelfAndCurrentWindow(t *testing.T) {
	t.Parallel()
	inputs := make(chan BodyBuildInput, 2)
	release := make(chan struct{})
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			<-release
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:body")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	input := <-inputs
	if input.ActorID != id || input.AttemptKey != key || input.Self.ID() != id {
		t.Fatalf("builder input = %#v", input)
	}
	if input.Current.IsCurrent() {
		t.Fatal("candidate reported current before publication/Start")
	}
	snapshot, ok := host.Inspect(id)
	building, _ := host.inspectTransitional(id)
	if !ok || !building || snapshot.Actual != ActualNone {
		t.Fatalf("building snapshot = %#v (building=%v)", snapshot, building)
	}
	close(release)
	eventually(t, func() bool {
		s, ok := host.Inspect(id)
		return ok && s.Actual == ActualBody && s.Unit != nil && s.Unit.IsAlive()
	})
	if !input.Current.IsCurrent() {
		t.Fatal("published exact candidate is not current")
	}
	snapshot, _ = host.Inspect(id)
	if snapshot.Unit.Self() != input.Self {
		t.Fatal("Prepare Self and published Unit Self differ")
	}
	if string(snapshot.Desired.(BodyDesired).ExecutionSpec.Config) != `{"a":1,"b":2}` {
		t.Fatalf("config was not canonicalized: %s", snapshot.Desired.(BodyDesired).ExecutionSpec.Config)
	}
}

func TestAcceptedLevelProbesSeparateIdentityAttemptAndPhysicalCurrent(t *testing.T) {
	t.Parallel()
	inputs := make(chan BodyBuildInput, 2)
	release := make(chan struct{})
	host, err := New(Config{
		Domain:       "daemon",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			<-release
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:probe-levels")
	g1 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	in1 := <-inputs
	if !in1.Identity.IsCurrent() {
		t.Fatal("accepted A was not identity-current before body publication")
	}
	if !in1.Attempt.IsCurrent() {
		t.Fatal("accepted A/G1 was not attempt-current before body publication")
	}
	if in1.Current.IsCurrent() {
		t.Fatal("candidate C1 was physical-current before publication")
	}

	g2 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	in2 := <-inputs
	if !in1.Identity.IsCurrent() {
		t.Fatal("A identity stopped being current across G1 to G2")
	}
	if in1.Attempt.IsCurrent() {
		t.Fatal("stale A/G1 remained attempt-current after accepting G2")
	}
	if !in2.Identity.IsCurrent() || !in2.Attempt.IsCurrent() {
		t.Fatal("accepted A/G2 probes were not current")
	}
	if in2.Current.IsCurrent() {
		t.Fatal("candidate C2 was physical-current before publication")
	}

	close(release)
	eventually(t, func() bool {
		snapshot, ok := host.Inspect(id)
		return ok && snapshot.Actual == ActualBody && snapshot.Attempt == g2 &&
			snapshot.Unit != nil && snapshot.Unit.IsAlive()
	})
	if in1.Current.IsCurrent() {
		t.Fatal("losing C1 became physical-current")
	}
	if !in2.Current.IsCurrent() {
		t.Fatal("published C2 did not become physical-current")
	}

	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	if in1.Identity.IsCurrent() || in2.Identity.IsCurrent() ||
		in1.Attempt.IsCurrent() || in2.Attempt.IsCurrent() {
		t.Fatal("accepted-level probes survived desired removal")
	}
}

func TestDirectReplacementKeepsPredecessorUntilCandidatePublishes(t *testing.T) {
	t.Parallel()
	inputs := make(chan BodyBuildInput, 4)
	releases := make(chan chan struct{}, 4)
	actors := make(chan *hostTestActor, 4)
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			gate := make(chan struct{})
			inputs <- input
			releases <- gate
			<-gate
			impl := newHostTestActor()
			actors <- impl
			return impl
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:replace")
	g1 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	in1 := <-inputs
	close(<-releases)
	<-actors
	eventually(t, func() bool {
		s, ok := host.Inspect(id)
		return ok && s.Actual == ActualBody && s.Attempt == g1 && s.Unit.IsAlive()
	})
	first, _ := host.Inspect(id)

	g2 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	in2 := <-inputs
	gate2 := <-releases
	during, _ := host.Inspect(id)
	duringBuilding, _ := host.inspectTransitional(id)
	if during.Actual != ActualBody || during.Attempt != g1 || during.Unit != first.Unit || !duringBuilding {
		t.Fatalf("predecessor was not kept during build: %#v (building=%v)", during, duringBuilding)
	}
	if in2.Current.IsCurrent() {
		t.Fatal("G2 current before publication")
	}
	close(gate2)
	<-actors
	eventually(t, func() bool {
		s, ok := host.Inspect(id)
		return ok && s.Actual == ActualBody && s.Attempt == g2 && s.Unit != first.Unit && s.Unit.IsAlive()
	})
	if in1.Current.IsCurrent() {
		t.Fatal("G1 remained current after G2 publication")
	}
	if !in2.Current.IsCurrent() {
		t.Fatal("G2 did not become current")
	}
	eventually(t, func() bool {
		_, retiring := host.inspectTransitional(id)
		return retiring == 0
	})
}

func TestNaturalExitRebuildsSameAttemptAndReapsRetiring(t *testing.T) {
	t.Parallel()
	actors := make(chan *hostTestActor, 4)
	inputs := make(chan BodyBuildInput, 4)
	events := newHostEventProbe()
	host, err := New(Config{
		Domain:       "daemon",
		PollInterval: 5 * time.Millisecond,
		Events:       events,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			impl := newHostTestActor()
			inputs <- input
			actors <- impl
			return impl
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)

	id := actor.ActorID("agent:natural")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	in1 := <-inputs
	a1 := <-actors
	eventually(t, func() bool { return in1.Current.IsCurrent() })
	first, _ := host.Inspect(id)
	cause := errors.New("actor exited")
	a1.dying <- cause
	got := <-events.exits
	if got.id != id || got.key != key || got.self != in1.Self || !errors.Is(got.cause, cause) {
		t.Fatalf("exit event = %#v", got)
	}
	in2 := <-inputs
	<-actors
	eventually(t, func() bool {
		s, _ := host.Inspect(id)
		_, retiring := host.inspectTransitional(id)
		return s.Actual == ActualBody && s.Attempt == key && s.Unit != first.Unit && retiring == 0
	})
	if in1.Current.IsCurrent() || !in2.Current.IsCurrent() {
		t.Fatal("same-attempt exact current did not move to rebuilt Unit")
	}
}

func TestAttachLastWinsStaleProtectionAndExactBindingDown(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:route")

	a := testAttempt(t)
	b := testAttempt(t)
	low, high := a, b
	if string(low) > string(high) {
		low, high = high, low
	}
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: low, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	b1 := newTestBinding()
	h1 := exactTestBinding(t, b1)
	if err := host.Attach(id, low, h1); err != nil {
		t.Fatal(err)
	}
	b2 := newTestBinding()
	h2 := exactTestBinding(t, b2)
	if err := host.Attach(id, low, h2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b1.closed:
	default:
		t.Fatal("same-attempt predecessor was not signaled closed")
	}
	b3 := newTestBinding()
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: high, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	h3 := exactTestBinding(t, b3)
	if err := host.Attach(id, high, h3); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b2.closed:
	default:
		t.Fatal("cross-attempt predecessor was not signaled closed")
	}
	stale := newTestBinding()
	staleHandle := exactTestBinding(t, stale)
	// A newer attempt holds the route, so this one is refused as superseded —
	// never as "not yet", which would tell the caller to keep trying.
	if err := host.Attach(id, low, staleHandle); !errors.Is(err, ErrAttachStale) {
		t.Fatalf("stale attach error = %v", err)
	}
	select {
	case <-stale.closed:
		t.Fatal("Host took ownership of rejected incoming Binding")
	default:
	}
	host.BindingDown(id, h2)
	snapshot, _ := host.Inspect(id)
	if snapshot.Binding != h3 {
		t.Fatal("stale BindingDown removed successor")
	}
	host.BindingDown(id, h3)
	snapshot, ok := host.Inspect(id)
	if ok && snapshot.Actual != ActualNone {
		t.Fatalf("exact BindingDown left route: %#v", snapshot)
	}
	stale.finish()
	b1.finish()
	b2.finish()
	b3.finish()
}

// A refused attach says one of two opposite things, and the caller redials on
// one and gives up on the other. Permission is not among them: by the time
// Attach runs the Controller has already authorized this actor, attempt and
// peer, so everything read here is this host's own desired — a projection that
// converges on its own clock. Both refusals are statements about how far along
// this host is.
//
// The distinction is the point, so this pins the two against each other rather
// than each on its own: a single error value for both is what made a projection
// that had not caught up indistinguishable from an attempt that lost.
func TestAttachSeparatesNotConvergedFromSuperseded(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:attach-verdicts")

	a, b := testAttempt(t), testAttempt(t)
	low, high := a, b
	if string(low) > string(high) {
		low, high = high, low
	}

	// Nothing desired here at all yet: the Controller authorized a placement
	// this host has not been told about. Retrying is exactly right.
	early := newTestBinding()
	defer early.finish()
	if err := host.Attach(id, high, exactTestBinding(t, early)); !errors.Is(err, ErrAttachNotReady) {
		t.Fatalf("attach before any desired = %v, want not-ready", err)
	}

	// Desired names the OLDER attempt. The incoming newer one is still ahead of
	// this host, not behind it — also retryable.
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: low, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	ahead := newTestBinding()
	defer ahead.finish()
	if err := host.Attach(id, high, exactTestBinding(t, ahead)); !errors.Is(err, ErrAttachNotReady) {
		t.Fatalf("attach ahead of desired = %v, want not-ready", err)
	}

	// Now the newer attempt is desired AND holds the route. The older one has
	// lost for good, and must not be told to keep trying.
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: high, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	winner := newTestBinding()
	defer winner.finish()
	if err := host.Attach(id, high, exactTestBinding(t, winner)); err != nil {
		t.Fatalf("the desired attempt was refused its route: %v", err)
	}
	loser := newTestBinding()
	defer loser.finish()
	lost := host.Attach(id, low, exactTestBinding(t, loser))
	if !errors.Is(lost, ErrAttachStale) {
		t.Fatalf("superseded attach = %v, want stale", lost)
	}
	// The whole point: one value cannot satisfy both readings.
	if errors.Is(lost, ErrAttachNotReady) {
		t.Fatal("the two refusals are the same value again")
	}

	// A closed host is not judging the attach at all, and says so in the words
	// every other entry point on this type uses.
	if err := host.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	sealed := newTestBinding()
	defer sealed.finish()
	if err := host.Attach(id, high, exactTestBinding(t, sealed)); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("attach on a closed host = %v, want host-closed", err)
	}
}

func TestOpaqueBindingIdentityAcceptsNonComparableResource(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:non-comparable-binding")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: key, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	resource := nonComparableBinding{payload: []byte("slice makes this value non-comparable"), done: make(chan struct{})}
	binding := exactTestBinding(t, resource)
	if err := host.Attach(id, key, binding); err != nil {
		t.Fatal(err)
	}
	if err := host.Attach(id, key, binding); err != nil {
		t.Fatal(err)
	}
	host.BindingDown(id, binding)
	if snapshot, ok := host.Inspect(id); ok && snapshot.Actual != ActualNone {
		t.Fatalf("exact opaque BindingDown left route: %#v", snapshot)
	}
}

func TestAttachDuringBodyBuildIsRetryableAndDoesNotOwnIncoming(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	host, err := New(Config{
		Domain:       "daemon",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(BodyBuildInput) actorrt.Actor {
			close(started)
			<-release
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:building")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	<-started
	binding := newTestBinding()
	handle := exactTestBinding(t, binding)
	// A build is in flight for this id. Retiring it is this host's own
	// convergence work, so the refusal is "not yet" — the caller that redials
	// gets in once the build settles.
	if err := host.Attach(id, key, handle); !errors.Is(err, ErrAttachNotReady) {
		t.Fatalf("Attach error = %v", err)
	}
	select {
	case <-binding.closed:
		t.Fatal("Host closed rejected incoming Binding")
	default:
	}
	binding.finish()
	close(release)
}

func TestEndpointInvocationUsesOneSlidingWindowSnapshot(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:window")
	key := testAttempt(t)

	if err := host.AcceptFullDesired([]Desired{CarrierDesired{
		ActorID: id, AttemptKey: key, PeerDomain: "daemon",
	}}); err != nil {
		t.Fatal(err)
	}
	b1 := newTestBinding()
	b1.block = make(chan struct{})
	if err := host.Attach(id, key, exactTestBinding(t, b1)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- host.Deliver(id, &message.Envelope{ID: "old"})
	}()
	eventually(t, func() bool { return b1.calls.Load() == 1 })

	b2 := newTestBinding()
	if err := host.Attach(id, key, exactTestBinding(t, b2)); err != nil {
		t.Fatal(err)
	}
	if err := host.Deliver(id, &message.Envelope{ID: "new"}); err != nil {
		t.Fatal(err)
	}
	if got := <-b2.deliver; got != "new" {
		t.Fatalf("successor received %q", got)
	}
	close(b1.block)
	if err := <-result; err == nil {
		t.Fatal("in-flight predecessor invocation should report its own closed result")
	}
	if b1.calls.Load() != 1 || b2.calls.Load() != 1 {
		t.Fatalf("calls: predecessor=%d successor=%d", b1.calls.Load(), b2.calls.Load())
	}
	b1.finish()
	b2.finish()
}

func TestDesiredRemovalRetiresBodyAndDeletesSparseRow(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:remove")
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, testAttempt(t))}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		s, ok := host.Inspect(id)
		return ok && s.Actual == ActualBody && s.Unit.IsAlive()
	})
	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, ok := host.Inspect(id)
		return !ok
	})
}

func TestDesiredChangeDuringPrepareMakesExactBuildLoser(t *testing.T) {
	t.Parallel()
	inputs := make(chan BodyBuildInput, 2)
	release := make(chan struct{})
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input BodyBuildInput) actorrt.Actor {
			inputs <- input
			<-release
			return newHostTestActor()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:build-loser")
	g1 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	in1 := <-inputs
	g2 := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{
		CarrierDesired{ActorID: id, AttemptKey: g2, PeerDomain: "daemon"},
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	eventually(t, func() bool {
		snapshot, ok := host.Inspect(id)
		building, retiring := host.inspectTransitional(id)
		return ok &&
			snapshot.Desired.(CarrierDesired).AttemptKey == g2 &&
			snapshot.Actual == ActualNone &&
			!building &&
			retiring == 0
	})
	if in1.Current.IsCurrent() {
		t.Fatal("invalidated candidate became current")
	}
}

func TestHighChurnRetiringSetReturnsToZero(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:       "server",
		PollInterval: 2 * time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	id := actor.ActorID("agent:churn")
	var previous *actorrt.Unit
	for i := 0; i < 40; i++ {
		key := testAttempt(t)
		if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool {
			snapshot, ok := host.Inspect(id)
			return ok &&
				snapshot.Actual == ActualBody &&
				snapshot.Attempt == key &&
				snapshot.Unit != previous &&
				snapshot.Unit.IsAlive()
		})
		snapshot, _ := host.Inspect(id)
		previous = snapshot.Unit
	}
	eventually(t, func() bool {
		_, ok := host.Inspect(id)
		_, retiring := host.inspectTransitional(id)
		return ok && retiring == 0
	})
}

func TestCloseDoesNotReportAlreadyDoneRetiringUnit(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:       "server",
		PollInterval: time.Millisecond,
		BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	id := actor.ActorID("agent:done-before-close")
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		snapshot, ok := host.Inspect(id)
		return ok && snapshot.Actual == ActualBody
	})
	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, ok := host.Inspect(id)
		return !ok
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("Close reported an already-Done retiring unit: %v", err)
	}
}

func TestHostCoreConformanceOnServerAndDaemonDomains(t *testing.T) {
	for _, domain := range []ExecutionDomain{"server", "daemon"} {
		domain := domain
		t.Run(string(domain), func(t *testing.T) {
			t.Parallel()
			host, err := New(Config{
				Domain:       domain,
				PollInterval: 5 * time.Millisecond,
				BodyBuilder:  func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer closeHost(t, host)
			id := actor.ActorID("agent:conformance-" + string(domain))
			key := testAttempt(t)
			if err := host.AcceptFullDesired([]Desired{bodyDesiredFor(t, id, key)}); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool {
				snapshot, ok := host.Inspect(id)
				return ok && snapshot.Actual == ActualBody && snapshot.Unit.IsAlive()
			})
			if err := host.Deliver(id, &message.Envelope{ID: "m"}); err != nil {
				t.Fatal(err)
			}
			if err := host.AcceptFullDesired(nil); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool {
				_, ok := host.Inspect(id)
				return !ok
			})
		})
	}
}

// The kernel needs no special rejection branch here: it is never a member, so
// the value ledger never emits a system coordinate and an unhosted id simply
// answers "not hosted" through the ordinary path.
func TestSystemActorIsSimplyNotHosted(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	if err := host.Deliver(actor.SystemActorID, &message.Envelope{ID: "x"}); !errors.Is(err, ErrNotHosted) {
		t.Fatalf("deliver error = %v", err)
	}
}
