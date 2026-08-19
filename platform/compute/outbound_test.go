package compute

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// This file, outbound_cancel_test.go and outbound_obs_routing_test.go are the
// ported form of four pre-device-model-refactor test files (outbound_test.go,
// outbound_cancel_test.go, outbound_obs_routing_test.go,
// session_hardening_test.go). The old fixtures dialed a real websocket and
// authenticated a link.AuthenticatedLinkSession backed by a
// link.RemoteSessionLedger; DaemonOutbound now depends on nothing wider than
// the two small interfaces it declares itself (LaneSession/laneActorStream),
// so every fixture below is an in-package fake of those two interfaces
// instead of a wire dial. See the final report for the full old-test
// disposition list (ported vs. abandoned-with-reason).

// ---------------------------------------------------------------------------
// outboundProbe: the fake behind the five capability arms and PublishObs,
// shared by every file in this trio.
// ---------------------------------------------------------------------------

type outboundProbe struct {
	penCalls       atomic.Int64
	accessCalls    atomic.Int64
	stateCalls     atomic.Int64
	scheduleCalls  atomic.Int64
	lifecycleCalls atomic.Int64

	penStarted chan struct{}
	penRelease chan struct{}
	penErr     error
	startOnce  sync.Once

	obsMu   sync.Mutex
	obsSeen []probeObs
}

type probeObs struct {
	kind  string
	value string
}

func (p *outboundProbe) recordObs(kind string, value []byte) error {
	p.obsMu.Lock()
	defer p.obsMu.Unlock()
	p.obsSeen = append(p.obsSeen, probeObs{kind: kind, value: string(value)})
	return nil
}

func (p *outboundProbe) observations() []probeObs {
	p.obsMu.Lock()
	defer p.obsMu.Unlock()
	return append([]probeObs(nil), p.obsSeen...)
}

type outboundProbePen struct{ probe *outboundProbe }

func (p outboundProbePen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.probe.penCalls.Add(1)
	if p.probe.penStarted != nil {
		p.probe.startOnce.Do(func() { close(p.probe.penStarted) })
	}
	if p.probe.penRelease != nil {
		<-p.probe.penRelease
	}
	result := harness.WriteResult{}
	if env != nil {
		result.MessageID = env.ID
	}
	return result, p.probe.penErr
}

type outboundProbeState struct{ probe *outboundProbe }

func (p outboundProbeState) Invoke(context.Context, access.Operation, resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	p.probe.stateCalls.Add(1)
	return accessdoor.Outcome{}, nil
}

type outboundProbeAccess struct{ probe *outboundProbe }

func (p outboundProbeAccess) Invoke(context.Context, access.Operation, resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.Outcome{}, nil
}
func (p outboundProbeAccess) Create(context.Context, resource.ResourceID, accessdoor.CreateSpec, []byte) (accessdoor.Outcome, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.Outcome{}, nil
}
func (p outboundProbeAccess) Stat(context.Context, resource.ResourceID) (accessdoor.StatResult, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.StatResult{}, nil
}
func (p outboundProbeAccess) List(context.Context, accessdoor.ListQuery) (accessdoor.ListPage, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.ListPage{}, nil
}
func (p outboundProbeAccess) Open(context.Context, resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, nil
}
func (p outboundProbeAccess) Redeem(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	p.probe.accessCalls.Add(1)
	return accessdoor.FileAccess{}, nil
}

type outboundProbeSchedule struct{ probe *outboundProbe }

func (p outboundProbeSchedule) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	p.probe.scheduleCalls.Add(1)
	return "timer", nil
}
func (p outboundProbeSchedule) Cancel(context.Context, schedule.TimerID) error {
	p.probe.scheduleCalls.Add(1)
	return nil
}
func (p outboundProbeSchedule) Ack(context.Context, schedule.TimerID) error {
	p.probe.scheduleCalls.Add(1)
	return nil
}

type outboundProbeLifecycle struct{ probe *outboundProbe }

func (p outboundProbeLifecycle) EndSelf(context.Context, actorcaps.EndSelfRequest) error {
	p.probe.lifecycleCalls.Add(1)
	return nil
}

func (p *outboundProbe) arms() link.RawActorArms {
	return link.RawActorArms{
		Pen:       outboundProbePen{probe: p},
		Access:    outboundProbeAccess{probe: p},
		State:     outboundProbeState{probe: p},
		Schedule:  outboundProbeSchedule{probe: p},
		Lifecycle: outboundProbeLifecycle{probe: p},
	}
}

var _ accessdoor.ResourceAccessHandle = outboundProbeAccess{}

// ---------------------------------------------------------------------------
// fakeOutboundStream / outboundStreamFactory: laneActorStream fakes.
// ---------------------------------------------------------------------------

type fakeOutboundStream struct {
	probe *outboundProbe
	done  chan struct{}
	once  sync.Once
}

func (s *fakeOutboundStream) Arms() link.RawActorArms { return s.probe.arms() }
func (s *fakeOutboundStream) Done() <-chan struct{}   { return s.done }
func (s *fakeOutboundStream) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}
func (s *fakeOutboundStream) SendCancelRequest(message.ID) error { return nil }
func (s *fakeOutboundStream) PublishObs(kind string, value []byte) error {
	return s.probe.recordObs(kind, value)
}

var _ laneActorStream = (*fakeOutboundStream)(nil)

// outboundStreamFactory hands out one fakeOutboundStream per open call, index
// by index against a preset probe list (a probe list shorter than the number
// of opens mints a fresh probe for the overflow).
type outboundStreamFactory struct {
	mu       sync.Mutex
	probes   []*outboundProbe
	streams  []*fakeOutboundStream
	openGate chan struct{}
	openErr  error
	opens    atomic.Int64
}

func (f *outboundStreamFactory) open(
	_ context.Context,
	_ actor.ActorID,
	_ actorhost.AttemptKey,
) (laneActorStream, error) {
	if f.openGate != nil {
		<-f.openGate
	}
	index := int(f.opens.Add(1) - 1)
	if f.openErr != nil {
		return nil, f.openErr
	}
	f.mu.Lock()
	var probe *outboundProbe
	if index < len(f.probes) {
		probe = f.probes[index]
	} else {
		probe = &outboundProbe{}
		f.probes = append(f.probes, probe)
	}
	stream := &fakeOutboundStream{probe: probe, done: make(chan struct{})}
	f.streams = append(f.streams, stream)
	f.mu.Unlock()
	return stream, nil
}

func (f *outboundStreamFactory) finish(index int) {
	f.mu.Lock()
	stream := f.streams[index]
	f.mu.Unlock()
	_ = stream.Close()
}

// ---------------------------------------------------------------------------
// fakeLaneSession: LaneSession fake.
// ---------------------------------------------------------------------------

type fakeLaneSession struct {
	mu      sync.Mutex
	current bool
	done    chan struct{}
	doneOne sync.Once
	opener  func(context.Context, actor.ActorID, actorhost.AttemptKey) (laneActorStream, error)
}

func newFakeLaneSession(
	opener func(context.Context, actor.ActorID, actorhost.AttemptKey) (laneActorStream, error),
) *fakeLaneSession {
	return &fakeLaneSession{current: true, done: make(chan struct{}), opener: opener}
}

func newOutboundSession(factory *outboundStreamFactory) *fakeLaneSession {
	return newFakeLaneSession(factory.open)
}

func (s *fakeLaneSession) IsCurrent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

func (s *fakeLaneSession) Done() <-chan struct{} { return s.done }

func (s *fakeLaneSession) OpenActorStream(
	ctx context.Context, id actor.ActorID, key actorhost.AttemptKey,
) (laneActorStream, error) {
	return s.opener(ctx, id, key)
}

// kill is what a superseded/lost lane looks like from DaemonOutbound's point
// of view: it stops answering IsCurrent and its Done channel closes.
func (s *fakeLaneSession) kill() {
	s.mu.Lock()
	s.current = false
	s.mu.Unlock()
	s.doneOne.Do(func() { close(s.done) })
}

var _ LaneSession = (*fakeLaneSession)(nil)

// ---------------------------------------------------------------------------
// Host/build fixtures shared by every DaemonOutbound test.
// ---------------------------------------------------------------------------

type outboundTestActor struct {
	stopPanic bool
}

func (*outboundTestActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a *outboundTestActor) Stop(context.Context) error {
	if a.stopPanic {
		panic("stop panic")
	}
	return nil
}

type outboundBuild struct {
	prepared PreparedOutbound
	input    actorhost.BodyBuildInput
	release  chan struct{}
}

func outboundAttempt(t *testing.T) actorhost.AttemptKey {
	t.Helper()
	key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func outboundDesired(t *testing.T, id actor.ActorID, key actorhost.AttemptKey) actorhost.BodyDesired {
	t.Helper()
	return actorhost.BodyDesired{
		ActorID:    id,
		AttemptKey: key,
		ExecutionSpec: actorhost.ExecutionSpec{
			Kind:  actor.KindAgent,
			Class: "outbound-test",
		},
	}
}

func eventuallyOutbound(t *testing.T, check func() bool) {
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

func newOutboundHost(
	t *testing.T,
	outbound *DaemonOutbound,
	builds chan<- outboundBuild,
	stopPanic bool,
) *actorhost.HostSupervisor {
	t.Helper()
	host, err := actorhost.New(actorhost.Config{
		Domain:       "daemon",
		PollInterval: 5 * time.Millisecond,
		BodyBuilder: func(input actorhost.BodyBuildInput) actorrt.Actor {
			prepared, err := outbound.Prepare(
				input.ActorID,
				input.AttemptKey,
				input.Self,
				input.Identity,
				input.Attempt,
				input.Current,
			)
			if err != nil {
				panic(err)
			}
			release := make(chan struct{})
			builds <- outboundBuild{prepared: prepared, input: input, release: release}
			<-release
			return prepared.Wrap(&outboundTestActor{stopPanic: stopPanic})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func closeOutboundFixture(
	t *testing.T,
	host *actorhost.HostSupervisor,
	outbound *DaemonOutbound,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if host != nil {
		if err := host.Close(ctx); err != nil {
			t.Fatalf("host Close: %v", err)
		}
	}
	if outbound != nil {
		if err := outbound.Close(ctx); err != nil {
			t.Fatalf("outbound Close: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests ported from outbound_test.go.old
// ---------------------------------------------------------------------------

func TestOutboundSlotStartsFailClosedThenPublishesFiveArmsAtomically(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	factory := &outboundStreamFactory{probes: []*outboundProbe{{}}}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:arms")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	if _, err := build.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "before"}); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("pre-publication Write error = %v", err)
	}
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if _, err := build.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "offline"}); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("offline Write error = %v", err)
	}
	outcome, err := build.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:x", nil)
	if err != nil || outcome.RejectReason != access.OutcomeUnknown {
		t.Fatalf("offline access = %#v, %v", outcome, err)
	}

	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		bundle := build.prepared.Slot.arms.Load()
		return bundle != nil && bundle.Session == session && bundle.Stream != nil
	})
	probe := factory.probes[0]
	if _, err := build.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "pen"}); err != nil {
		t.Fatal(err)
	}
	if _, err := build.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := build.prepared.Caps.State.Invoke(t.Context(), access.OpRead, "resource:s", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := build.prepared.Caps.Schedule.Schedule(t.Context(), schedule.ScheduleReq{}); err != nil {
		t.Fatal(err)
	}
	if err := build.prepared.Caps.Lifecycle.EndSelf(t.Context(), actorcaps.EndSelfRequest{}); err != nil {
		t.Fatal(err)
	}
	if probe.penCalls.Load() != 1 ||
		probe.accessCalls.Load() != 1 ||
		probe.stateCalls.Load() != 1 ||
		probe.scheduleCalls.Load() != 1 ||
		probe.lifecycleCalls.Load() != 1 {
		t.Fatalf(
			"raw calls pen=%d access=%d state=%d schedule=%d lifecycle=%d",
			probe.penCalls.Load(),
			probe.accessCalls.Load(),
			probe.stateCalls.Load(),
			probe.scheduleCalls.Load(),
			probe.lifecycleCalls.Load(),
		)
	}
}

// TestOutboundLevelObsPublishedBeforeConnectReachesTheChannel pins the slot's
// half of the level contract. A level observation (device presence) answers
// "what is true right now", so a subscriber must end up seeing the current
// value — unlike an edge, which only reports that something happened.
//
// The body starts and its device connects the moment its port is up, which is
// BEFORE the daemon has finished opening this body's actor stream. The slot's
// whole reason to exist is that a body holds one stable arm across a
// replaceable stream, so a level published into that gap is the slot's to
// carry, not the body's to lose: nothing regenerates it (the device stays
// connected, so there is no second edge) and nothing re-reads it (obs is push
// only). Dropped here, the channel's presence view is wrong until the device
// disconnects and reconnects — which for a healthy device is never.
func TestOutboundLevelObsPublishedBeforeConnectReachesTheChannel(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	factory := &outboundStreamFactory{probes: []*outboundProbe{{}}}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("tool:device-holder")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)

	// The device connects here — body up, stream not yet open. This is the gap.
	outbound.publishObs(build.input.Self, actorrt.ObsKind("device_presence"), actorrt.ObsValue(`{"online":true}`))

	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	probe := factory.probes[0]
	eventuallyOutbound(t, func() bool {
		bundle := build.prepared.Slot.arms.Load()
		return bundle != nil && bundle.Session == session && bundle.Stream != nil
	})

	eventuallyOutbound(t, func() bool { return len(probe.observations()) > 0 })
	seen := probe.observations()
	if len(seen) != 1 {
		t.Fatalf("observations on connect = %+v, want exactly the one pending level", seen)
	}
	if seen[0].kind != "device_presence" || seen[0].value != `{"online":true}` {
		t.Fatalf("delivered observation = %+v, want the device_presence value published into the gap", seen[0])
	}
}

func TestOutboundReconnectDoesNotRetryInflightOrRebuildUnit(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)

	wantErr := errors.New("s1 write failed")
	p1 := &outboundProbe{
		penStarted: make(chan struct{}),
		penRelease: make(chan struct{}),
		penErr:     wantErr,
	}
	p2 := &outboundProbe{}
	f1 := &outboundStreamFactory{probes: []*outboundProbe{p1}}
	f2 := &outboundStreamFactory{probes: []*outboundProbe{p2}}
	s1 := newOutboundSession(f1)
	s2 := newOutboundSession(f2)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:reconnect")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetLane(s1); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		return build.prepared.Slot.arms.Load().Session == s1
	})
	before, _ := host.Inspect(id)

	result := make(chan error, 1)
	go func() {
		_, err := build.prepared.Caps.Pen.Write(context.Background(), &message.Envelope{ID: "inflight"})
		result <- err
	}()
	<-p1.penStarted
	if err := outbound.SetLane(s2); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		return build.prepared.Slot.arms.Load().Session == s2
	})
	close(p1.penRelease)
	if err := <-result; !errors.Is(err, wantErr) {
		t.Fatalf("in-flight result = %v", err)
	}
	if p1.penCalls.Load() != 1 || p2.penCalls.Load() != 0 {
		t.Fatalf("in-flight call retried: s1=%d s2=%d", p1.penCalls.Load(), p2.penCalls.Load())
	}
	if _, err := build.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "future"}); err != nil {
		t.Fatal(err)
	}
	if p2.penCalls.Load() != 1 {
		t.Fatalf("future call did not use S2: %d", p2.penCalls.Load())
	}
	// The invariant that used to be pinned at the wire/session-ledger layer
	// (session_hardening_test.go's TestCurrentLossNeverFallsBackToOlderActive
	// Session): once a session has been superseded, its own loss must never
	// resurrect it as current. Here that is s1 reporting Done() after s2 is
	// already current.
	outbound.LaneDown(s1)
	if bundle := build.prepared.Slot.arms.Load(); bundle.Session != s2 {
		t.Fatal("stale S1 down cleared S2 bundle")
	}
	after, _ := host.Inspect(id)
	if before.Unit != after.Unit || build.prepared.Slot.closed.Load() {
		t.Fatal("whole-session reconnect rebuilt Unit or slot")
	}
	select {
	case <-s1.Done():
		t.Fatal("DaemonOutbound took physical session ownership")
	default:
	}
}

func TestPausedOpenBecomesExactLoserAfterSessionChanges(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	openGate := make(chan struct{})
	f1 := &outboundStreamFactory{
		probes:   []*outboundProbe{{}},
		openGate: openGate,
	}
	f2 := &outboundStreamFactory{probes: []*outboundProbe{{}}}
	s1 := newOutboundSession(f1)
	s2 := newOutboundSession(f2)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:paused-open")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetLane(s1); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		build.prepared.Slot.owner.mu.Lock()
		opening := build.prepared.Slot.opening
		build.prepared.Slot.owner.mu.Unlock()
		return opening
	})
	if err := outbound.SetLane(s2); err != nil {
		t.Fatal(err)
	}
	close(openGate)
	eventuallyOutbound(t, func() bool {
		bundle := build.prepared.Slot.arms.Load()
		return bundle.Session == s2 && bundle.Stream != nil
	})
	eventuallyOutbound(t, func() bool {
		f1.mu.Lock()
		defer f1.mu.Unlock()
		return len(f1.streams) == 1 && channelClosed(f1.streams[0].Done())
	})
}

func TestOutboundReopensOneActorStreamWithoutReplacingUnit(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	p1 := &outboundProbe{}
	p2 := &outboundProbe{}
	factory := &outboundStreamFactory{probes: []*outboundProbe{p1, p2}}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:reopen")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool { return factory.opens.Load() == 1 })
	firstBundle := build.prepared.Slot.arms.Load()
	firstUnit, _ := host.Inspect(id)
	factory.finish(0)
	eventuallyOutbound(t, func() bool {
		bundle := build.prepared.Slot.arms.Load()
		return factory.opens.Load() >= 2 && bundle.Stream != nil && bundle.Stream != firstBundle.Stream
	})
	if _, err := build.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "after-reopen"}); err != nil {
		t.Fatal(err)
	}
	if p1.penCalls.Load() != 0 || p2.penCalls.Load() != 1 {
		t.Fatalf("reopened stream calls p1=%d p2=%d", p1.penCalls.Load(), p2.penCalls.Load())
	}
	afterUnit, _ := host.Inspect(id)
	if firstUnit.Unit != afterUnit.Unit || build.prepared.Slot.closed.Load() {
		t.Fatal("single-stream reopen replaced body Unit/slot")
	}
}

func TestExactG1SlotCleanupCannotHarmG2(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, true)
	factory := &outboundStreamFactory{probes: []*outboundProbe{{}, {}}}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}

	id := actor.ActorID("agent:g1-g2")
	g1 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	b1 := <-builds
	close(b1.release)
	eventuallyOutbound(t, func() bool {
		bundle := b1.prepared.Slot.arms.Load()
		return b1.input.Current.IsCurrent() && bundle.Session == session
	})

	g2 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	b2 := <-builds
	close(b2.release)
	eventuallyOutbound(t, func() bool {
		bundle := b2.prepared.Slot.arms.Load()
		return b2.input.Current.IsCurrent() && bundle.Session == session
	})
	eventuallyOutbound(t, func() bool { return b1.prepared.Slot.closed.Load() })
	if b2.prepared.Slot.closed.Load() {
		t.Fatal("G1 close-first Stop closed G2 slot")
	}
	if _, err := b2.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "g2"}); err != nil {
		t.Fatal(err)
	}
	outbound.mu.Lock()
	slotCount := len(outbound.slots)
	outbound.mu.Unlock()
	if slotCount != 1 {
		t.Fatalf("slot registry count = %d, want exact G2 only", slotCount)
	}
}

func TestAcceptedPlanReplacementFencesRunArmsButKeepsIdentityArms(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	probe := &outboundProbe{}
	factory := &outboundStreamFactory{probes: []*outboundProbe{probe, {}}}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}

	id := actor.ActorID("agent:authority-levels")
	g1 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	b1 := <-builds
	close(b1.release)
	eventuallyOutbound(t, func() bool {
		bundle := b1.prepared.Slot.arms.Load()
		return b1.input.Current.IsCurrent() && bundle.Session == session
	})

	g2 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	b2 := <-builds // keep G2 unpublished so the physical G1 slot remains open.

	if _, err := b1.prepared.Caps.Pen.Write(t.Context(), &message.Envelope{ID: "stale-run"}); !errors.Is(err, ErrOutboundNotCurrent) {
		t.Fatalf("G1 Pen after accepted G2 err=%v", err)
	}
	if _, err := b1.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:stale-run", nil); !errors.Is(err, ErrOutboundNotCurrent) {
		t.Fatalf("G1 Access after accepted G2 err=%v", err)
	}
	// Lifecycle is a run arm too. A body this daemon has already begun retiring
	// may not end its identity.
	if err := b1.prepared.Caps.Lifecycle.EndSelf(
		t.Context(), actorcaps.EndSelfRequest{},
	); !errors.Is(err, ErrOutboundNotCurrent) {
		t.Fatalf("G1 EndSelf after accepted G2 err=%v", err)
	}
	if _, err := b1.prepared.Caps.State.Invoke(t.Context(), access.OpRead, "resource:identity", nil); err != nil {
		t.Fatalf("G1 State lost A-level authority across replacement: %v", err)
	}
	if _, err := b1.prepared.Caps.Schedule.Schedule(t.Context(), schedule.ScheduleReq{}); err != nil {
		t.Fatalf("G1 Schedule lost A-level authority across replacement: %v", err)
	}
	if probe.penCalls.Load() != 0 || probe.accessCalls.Load() != 0 || probe.lifecycleCalls.Load() != 0 {
		t.Fatalf("stale run arms reached transport: pen=%d access=%d lifecycle=%d",
			probe.penCalls.Load(), probe.accessCalls.Load(), probe.lifecycleCalls.Load())
	}
	if probe.stateCalls.Load() != 1 || probe.scheduleCalls.Load() != 1 {
		t.Fatalf("identity arms did not reach transport: state=%d schedule=%d",
			probe.stateCalls.Load(), probe.scheduleCalls.Load())
	}

	close(b2.release)
	eventuallyOutbound(t, b2.input.Current.IsCurrent)
}

// TestDaemonOutboundCloseDoesNotOwnSession pins that DaemonOutbound never
// reaches for session teardown. Structurally this is now also guaranteed by
// the LaneSession interface shape (it exposes no Close method at all, so
// DaemonOutbound has literally no call it could make) — but this is worth
// keeping as a behavior test because it also pins that Close(ctx) returns
// promptly without waiting on the session's Done channel, which nothing in
// this test ever closes. A regression that made Seal/Close block on session
// completion would hang here instead of racing a real transport shutdown.
func TestDaemonOutboundCloseDoesNotOwnSession(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	factory := &outboundStreamFactory{}
	session := newOutboundSession(factory)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := outbound.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
		t.Fatal("DaemonOutbound closed the session it was handed")
	default:
	}
}

// TestOutboundSetLaneRejectsANonCurrentSessionAndNeverBecomesCurrent ports the
// DaemonOutbound-level half of what session_hardening_test.go's
// TestDaemonOutboundAcceptsOnlyLiveHomeMintedSessionAuthority pinned at the
// (now retired) wire-authentication layer: SetLane trusts LaneSession.
// IsCurrent() and refuses a session that already answers false, leaving
// DaemonOutbound's own current-session pointer untouched by the refusal.
func TestOutboundSetLaneRejectsANonCurrentSessionAndNeverBecomesCurrent(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	defer func() {
		if err := outbound.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	dead := newOutboundSession(&outboundStreamFactory{})
	dead.kill()
	if err := outbound.SetLane(dead); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("SetLane(non-current session) err = %v, want ErrOutboundDisconnected", err)
	}
	outbound.mu.Lock()
	installed := outbound.session
	outbound.mu.Unlock()
	if installed != nil {
		t.Fatal("a refused session was installed as current anyway")
	}

	live := newOutboundSession(&outboundStreamFactory{})
	if err := outbound.SetLane(live); err != nil {
		t.Fatal(err)
	}
	outbound.mu.Lock()
	installed = outbound.session
	outbound.mu.Unlock()
	if installed != live {
		t.Fatal("a live session after a refused one was not installed as current")
	}
}

func TestOutboundOpenFailureUsesBoundedRetryBackoff(t *testing.T) {
	t.Parallel()
	const retry = 80 * time.Millisecond
	outbound := NewDaemonOutbound(DaemonOutboundConfig{
		PollInterval: time.Millisecond,
		RetryDelay:   retry,
	})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	factory := &outboundStreamFactory{openErr: errors.New("open failed")}
	session := newOutboundSession(factory)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:open-backoff")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool { return factory.opens.Load() == 1 })
	time.Sleep(retry / 3)
	if got := factory.opens.Load(); got != 1 {
		t.Fatalf("failed stream open hot-spun %d attempts inside retry delay", got)
	}
	eventuallyOutbound(t, func() bool { return factory.opens.Load() >= 2 })
}

type panickingFactorySource struct {
	called chan struct{}
	once   sync.Once
}

func (s *panickingFactorySource) BuildClass(
	actor.ActorID,
	string,
	json.RawMessage,
) (platform.ActorFactory, bool) {
	s.once.Do(func() { close(s.called) })
	panic("factory build panic")
}

// selectiveFactorySource resolves some classes and refuses the rest, counting
// every ask — the shape of a daemon whose binary cannot build one row of the
// plan (version skew).
type selectiveFactorySource struct {
	mu    sync.Mutex
	calls map[string]int
}

func (s *selectiveFactorySource) BuildClass(
	_ actor.ActorID,
	class string,
	_ json.RawMessage,
) (platform.ActorFactory, bool) {
	s.mu.Lock()
	s.calls[class]++
	s.mu.Unlock()
	if class != "buildable" {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return nil
		}, nil
	}}}, true
}

func (s *selectiveFactorySource) count(class string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[class]
}

// One row this daemon cannot build fails alone: the buildable row converges to
// a live body while the unbuildable one retries on the Host's own backoff.
// This is the semantics the lazy factory shape was chosen FOR — the eager
// whole-plan rejection it replaced held every healthy row hostage to one bad
// one, in defence of old bodies that were already truth-dead.
func TestOneUnbuildableRowDoesNotBlockTheOthers(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: time.Millisecond})
	source := &selectiveFactorySource{calls: map[string]int{}}
	host, err := actorhost.New(actorhost.Config{
		Domain:       "daemon",
		PollInterval: time.Millisecond,
		RetryDelay:   time.Millisecond,
		BodyBuilder:  daemonBodyBuilder(outbound, source, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeOutboundFixture(t, host, outbound)

	good := actor.ActorID("agent:good")
	bad := actor.ActorID("agent:bad")
	goodDesired := outboundDesired(t, good, outboundAttempt(t))
	goodDesired.ExecutionSpec.Class = "buildable"
	badDesired := outboundDesired(t, bad, outboundAttempt(t))
	badDesired.ExecutionSpec.Class = "not-in-this-binary"
	if err := host.AcceptFullDesired([]actorhost.Desired{goodDesired, badDesired}); err != nil {
		t.Fatal(err)
	}

	eventuallyOutbound(t, func() bool {
		snapshot, ok := host.Inspect(good)
		return ok && snapshot.Actual == actorhost.ActualBody &&
			snapshot.Unit != nil && snapshot.Unit.IsAlive()
	})
	// The bad row keeps being retried — asked more than once — and never holds
	// a body; the good one was never rebuilt on its account.
	eventuallyOutbound(t, func() bool { return source.count("not-in-this-binary") >= 2 })
	if snapshot, ok := host.Inspect(bad); ok && snapshot.Actual == actorhost.ActualBody {
		t.Fatal("the unbuildable row acquired a body")
	}
}

func TestDaemonBodyBuildPanicClosesPreparedOutboundSlot(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: time.Millisecond})
	source := &panickingFactorySource{called: make(chan struct{})}
	host, err := actorhost.New(actorhost.Config{
		Domain:       "daemon",
		PollInterval: time.Millisecond,
		RetryDelay:   time.Hour,
		BodyBuilder:  daemonBodyBuilder(outbound, source, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:panic-after-slot")
	if err := host.AcceptFullDesired([]actorhost.Desired{
		outboundDesired(t, id, outboundAttempt(t)),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.called:
	case <-time.After(time.Second):
		t.Fatal("daemon body builder did not run")
	}
	eventuallyOutbound(t, func() bool {
		outbound.mu.Lock()
		defer outbound.mu.Unlock()
		return len(outbound.slots) == 0
	})
}

func TestDaemonShutdownSealsOutboundBeforeStoppingBodiesWithoutClosingTheirArms(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	probe := &outboundProbe{}
	factory := &outboundStreamFactory{probes: []*outboundProbe{probe}}
	session := newOutboundSession(factory)

	id := actor.ActorID("agent:shutdown-order")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetLane(session); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		bundle := build.prepared.Slot.arms.Load()
		return bundle != nil && bundle.Stream != nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := outbound.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := build.prepared.Caps.Pen.Write(ctx, &message.Envelope{ID: "after-seal"}); err != nil {
		t.Fatalf("Seal invalidated a still-running body's arms: %v", err)
	}
	if err := host.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !build.prepared.Slot.closed.Load() {
		t.Fatal("Host body Stop did not close its exact outbound slot")
	}
	if err := outbound.CloseResidual(); err != nil {
		t.Fatal(err)
	}
}
