package compute

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

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

func (p outboundProbeState) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
	p.probe.stateCalls.Add(1)
	return accessdoor.Outcome{}, nil
}

type outboundProbeAccess struct{ probe *outboundProbe }

func (p outboundProbeAccess) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
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

func (p outboundProbeLifecycle) Fork(context.Context, message.ID, actorcaps.ForkSpec) (actor.ActorID, error) {
	p.probe.lifecycleCalls.Add(1)
	return "agent:child", nil
}
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

type outboundStreamResource struct {
	probe *outboundProbe
	done  chan struct{}
	once  sync.Once
}

func (r *outboundStreamResource) close() error {
	r.once.Do(func() { close(r.done) })
	return nil
}

type outboundStreamFactory struct {
	mu        sync.Mutex
	probes    []*outboundProbe
	resources []*outboundStreamResource
	openGate  chan struct{}
	opens     atomic.Int64
}

func (f *outboundStreamFactory) open(
	_ context.Context,
	_ actor.ActorID,
	_ actorhost.AttemptKey,
) (link.ActorStreamResource, error) {
	if f.openGate != nil {
		<-f.openGate
	}
	index := int(f.opens.Add(1) - 1)
	f.mu.Lock()
	var probe *outboundProbe
	if index < len(f.probes) {
		probe = f.probes[index]
	} else {
		probe = &outboundProbe{}
		f.probes = append(f.probes, probe)
	}
	resource := &outboundStreamResource{probe: probe, done: make(chan struct{})}
	f.resources = append(f.resources, resource)
	f.mu.Unlock()
	return link.ActorStreamResource{
		Arms:  probe.arms(),
		Close: resource.close,
		Done:  resource.done,
	}, nil
}

func (f *outboundStreamFactory) finish(index int) {
	f.mu.Lock()
	resource := f.resources[index]
	f.mu.Unlock()
	_ = resource.close()
}

func newOutboundSession(t *testing.T, peer string, factory *outboundStreamFactory) *link.AuthenticatedLinkSession {
	t.Helper()
	session, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
		Peer:            actorhost.ExecutionDomain(peer),
		OpenActorStream: factory.open,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

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
	sessions ...*link.AuthenticatedLinkSession,
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
	for _, session := range sessions {
		if session == nil {
			continue
		}
		if err := session.Close(); err != nil {
			t.Fatalf("session Close: %v", err)
		}
		select {
		case <-session.Done():
		case <-ctx.Done():
			t.Fatal("session close timed out")
		}
	}
}

func TestOutboundSlotStartsFailClosedThenPublishesFiveArmsAtomically(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	factory := &outboundStreamFactory{probes: []*outboundProbe{{}}}
	session := newOutboundSession(t, "server", factory)
	defer closeOutboundFixture(t, host, outbound, session)

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
	outcome, err := build.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:x", nil, nil)
	if err != nil || outcome.RejectReason != access.OutcomeUnknown {
		t.Fatalf("offline access = %#v, %v", outcome, err)
	}

	if err := outbound.SetSession(session); err != nil {
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
	if _, err := build.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:a", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := build.prepared.Caps.State.Invoke(t.Context(), access.OpRead, "resource:s", nil, nil); err != nil {
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
	s1 := newOutboundSession(t, "server", f1)
	s2 := newOutboundSession(t, "server", f2)
	defer closeOutboundFixture(t, host, outbound, s1, s2)

	id := actor.ActorID("agent:reconnect")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetSession(s1); err != nil {
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
	if err := outbound.SetSession(s2); err != nil {
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
	outbound.SessionDown(s1)
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
	s1 := newOutboundSession(t, "server", f1)
	s2 := newOutboundSession(t, "server", f2)
	defer closeOutboundFixture(t, host, outbound, s1, s2)

	id := actor.ActorID("agent:paused-open")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetSession(s1); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		build.prepared.Slot.owner.mu.Lock()
		opening := build.prepared.Slot.opening
		build.prepared.Slot.owner.mu.Unlock()
		return opening
	})
	if err := outbound.SetSession(s2); err != nil {
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
		return len(f1.resources) == 1 && channelClosed(f1.resources[0].done)
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
	session := newOutboundSession(t, "server", factory)
	defer closeOutboundFixture(t, host, outbound, session)

	id := actor.ActorID("agent:reopen")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetSession(session); err != nil {
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
	session := newOutboundSession(t, "server", factory)
	defer closeOutboundFixture(t, host, outbound, session)
	if err := outbound.SetSession(session); err != nil {
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
	factory := &outboundStreamFactory{probes: []*outboundProbe{probe, &outboundProbe{}}}
	session := newOutboundSession(t, "server", factory)
	defer closeOutboundFixture(t, host, outbound, session)
	if err := outbound.SetSession(session); err != nil {
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
	if _, err := b1.prepared.Caps.Access.Invoke(t.Context(), access.OpRead, "resource:stale-run", nil, nil); !errors.Is(err, ErrOutboundNotCurrent) {
		t.Fatalf("G1 Access after accepted G2 err=%v", err)
	}
	if _, err := b1.prepared.Caps.State.Invoke(t.Context(), access.OpRead, "resource:identity", nil, nil); err != nil {
		t.Fatalf("G1 State lost A-level authority across replacement: %v", err)
	}
	if _, err := b1.prepared.Caps.Schedule.Schedule(t.Context(), schedule.ScheduleReq{}); err != nil {
		t.Fatalf("G1 Schedule lost A-level authority across replacement: %v", err)
	}
	if probe.penCalls.Load() != 0 || probe.accessCalls.Load() != 0 {
		t.Fatalf("stale run arms reached transport: pen=%d access=%d",
			probe.penCalls.Load(), probe.accessCalls.Load())
	}
	if probe.stateCalls.Load() != 1 || probe.scheduleCalls.Load() != 1 {
		t.Fatalf("identity arms did not reach transport: state=%d schedule=%d",
			probe.stateCalls.Load(), probe.scheduleCalls.Load())
	}

	close(b2.release)
	eventuallyOutbound(t, b2.input.Current.IsCurrent)
}

func TestDaemonOutboundCloseDoesNotOwnSession(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	factory := &outboundStreamFactory{}
	session := newOutboundSession(t, "server", factory)
	if err := outbound.SetSession(session); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := outbound.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
		t.Fatal("DaemonOutbound closed outer physical session")
	default:
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	<-session.Done()
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
	var opens atomic.Int64
	session, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
		Peer: "server",
		OpenActorStream: func(
			context.Context,
			actor.ActorID,
			actorhost.AttemptKey,
		) (link.ActorStreamResource, error) {
			opens.Add(1)
			return link.ActorStreamResource{}, errors.New("open failed")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeOutboundFixture(t, host, outbound, session)

	id := actor.ActorID("agent:open-backoff")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetSession(session); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool { return opens.Load() == 1 })
	time.Sleep(retry / 3)
	if got := opens.Load(); got != 1 {
		t.Fatalf("failed stream open hot-spun %d attempts inside retry delay", got)
	}
	eventuallyOutbound(t, func() bool { return opens.Load() >= 2 })
}

type panickingPlanSource struct {
	called chan struct{}
	once   sync.Once
}

func (*panickingPlanSource) ApplyPlan([]platform.PlanActor) error { return nil }
func (s *panickingPlanSource) LookupExact(
	actor.ActorID,
	actorhost.AttemptKey,
	actorhost.ExecutionSpec,
) (platform.ActorFactory, bool) {
	s.once.Do(func() { close(s.called) })
	panic("factory lookup panic")
}

func TestDaemonBodyBuildPanicClosesPreparedOutboundSlot(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: time.Millisecond})
	source := &panickingPlanSource{called: make(chan struct{})}
	host, err := actorhost.New(actorhost.Config{
		Domain:       "daemon",
		PollInterval: time.Millisecond,
		RetryDelay:   time.Hour,
		BodyBuilder:  daemonBodyBuilder(outbound, source),
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
	session := newOutboundSession(t, "server", factory)

	id := actor.ActorID("agent:shutdown-order")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	if err := outbound.SetSession(session); err != nil {
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
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
	case <-ctx.Done():
		t.Fatal("session close timed out")
	}
}

var _ accessdoor.ResourceAccessHandle = outboundProbeAccess{}
