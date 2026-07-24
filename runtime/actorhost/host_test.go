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
	got, err := CompareAttemptKeys(left, right)
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
	if !ok || !snapshot.Building || snapshot.Actual != ActualNone {
		t.Fatalf("building snapshot = %#v", snapshot)
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
	if during.Actual != ActualBody || during.Attempt != g1 || during.Unit != first.Unit || !during.Building {
		t.Fatalf("predecessor was not kept during build: %#v", during)
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
		s, _ := host.Inspect(id)
		return s.Retiring == 0
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
		return s.Actual == ActualBody && s.Attempt == key && s.Unit != first.Unit && s.Retiring == 0
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
	if err := host.Attach(id, low, b1); err != nil {
		t.Fatal(err)
	}
	b2 := newTestBinding()
	if err := host.Attach(id, low, b2); err != nil {
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
	if err := host.Attach(id, high, b3); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b2.closed:
	default:
		t.Fatal("cross-attempt predecessor was not signaled closed")
	}
	stale := newTestBinding()
	if err := host.Attach(id, low, stale); !errors.Is(err, ErrStaleBinding) {
		t.Fatalf("stale attach error = %v", err)
	}
	select {
	case <-stale.closed:
		t.Fatal("Host took ownership of rejected incoming Binding")
	default:
	}
	host.BindingDown(id, b2)
	snapshot, _ := host.Inspect(id)
	if snapshot.Binding != b3 {
		t.Fatal("stale BindingDown removed successor")
	}
	host.BindingDown(id, b3)
	snapshot, ok := host.Inspect(id)
	if ok && snapshot.Actual != ActualNone {
		t.Fatalf("exact BindingDown left route: %#v", snapshot)
	}
	stale.finish()
	b1.finish()
	b2.finish()
	b3.finish()
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
	if err := host.Attach(id, key, binding); !errors.Is(err, ErrAttachRetryable) {
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
	if err := host.Attach(id, key, b1); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- host.Deliver(id, &message.Envelope{ID: "old"})
	}()
	eventually(t, func() bool { return b1.calls.Load() == 1 })

	b2 := newTestBinding()
	if err := host.Attach(id, key, b2); err != nil {
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
		return ok &&
			snapshot.Desired.(CarrierDesired).AttemptKey == g2 &&
			snapshot.Actual == ActualNone &&
			!snapshot.Building &&
			snapshot.Retiring == 0
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
		snapshot, ok := host.Inspect(id)
		return ok && snapshot.Retiring == 0
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

func TestSystemActorRejectedAtHostBoundaries(t *testing.T) {
	t.Parallel()
	host, err := New(Config{
		Domain:      "server",
		BodyBuilder: func(BodyBuildInput) actorrt.Actor { return newHostTestActor() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeHost(t, host)
	key := testAttempt(t)
	if err := host.AcceptFullDesired([]Desired{
		bodyDesiredFor(t, actor.SystemActorID, key),
	}); !errors.Is(err, ErrReservedSystem) {
		t.Fatalf("desired error = %v", err)
	}
	binding := newTestBinding()
	if err := host.Attach(actor.SystemActorID, key, binding); !errors.Is(err, ErrReservedSystem) {
		t.Fatalf("attach error = %v", err)
	}
	binding.finish()
	if err := host.Deliver(actor.SystemActorID, &message.Envelope{ID: "x"}); !errors.Is(err, ErrNotHosted) {
		t.Fatalf("deliver error = %v", err)
	}
}
