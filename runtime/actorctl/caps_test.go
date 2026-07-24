package actorctl

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// recordingPen counts raw writes. entered/release, when non-nil, let a test pin
// the sliding-window "in-flight call reaches raw exactly once" behaviour: the
// pen signals it has passed the gate (entered), then blocks in raw (release)
// while the test commits a terminal, and only then counts.
type recordingPen struct {
	writes  atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (p *recordingPen) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	if p.entered != nil {
		p.entered <- struct{}{}
	}
	if p.release != nil {
		<-p.release
	}
	p.writes.Add(1)
	return harness.WriteResult{}, nil
}

type recordingAccess struct{ invokes atomic.Int64 }

func (a *recordingAccess) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
	a.invokes.Add(1)
	return accessdoor.Outcome{}, nil
}

type recordingResourceAccess struct{ calls atomic.Int64 }

func (a *recordingResourceAccess) Invoke(context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant) (accessdoor.Outcome, error) {
	a.calls.Add(1)
	return accessdoor.Outcome{}, nil
}

func (a *recordingResourceAccess) Create(context.Context, resource.ResourceID, accessdoor.CreateSpec, []byte) (accessdoor.Outcome, error) {
	a.calls.Add(1)
	return accessdoor.Outcome{}, nil
}

func (a *recordingResourceAccess) Stat(context.Context, resource.ResourceID) (accessdoor.StatResult, error) {
	a.calls.Add(1)
	return accessdoor.StatResult{}, nil
}

func (a *recordingResourceAccess) List(context.Context, accessdoor.ListQuery) (accessdoor.ListPage, error) {
	a.calls.Add(1)
	return accessdoor.ListPage{}, nil
}

func (a *recordingResourceAccess) Open(context.Context, resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	a.calls.Add(1)
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, nil
}

func (a *recordingResourceAccess) Redeem(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	a.calls.Add(1)
	return accessdoor.FileAccess{}, nil
}

type recordingSchedule struct{ calls atomic.Int64 }

func (s *recordingSchedule) Schedule(context.Context, schedule.ScheduleReq) (schedule.TimerID, error) {
	s.calls.Add(1)
	return "", nil
}

func (s *recordingSchedule) Cancel(context.Context, schedule.TimerID) error {
	s.calls.Add(1)
	return nil
}

func (s *recordingSchedule) Ack(context.Context, schedule.TimerID) error {
	s.calls.Add(1)
	return nil
}

type fakePenMinter struct{ pen *recordingPen }

func (m fakePenMinter) Mint(actor.ActorID, actor.Kind, channel.ID, int64) harness.Pen {
	return m.pen
}

type fakeAccessMinter struct{ handle *recordingResourceAccess }

func (m fakeAccessMinter) Mint(storespec.AuthorStamp) accessdoor.ResourceAccessHandle {
	return m.handle
}

type fakeScheduleMinter struct{ handle *recordingSchedule }

func (m fakeScheduleMinter) MintCurrent(storespec.AuthorStamp, func() bool) schedule.ScheduleHandle {
	return m.handle
}

type fakeStateResolver struct{ handle *recordingAccess }

func (r fakeStateResolver) Resolve(context.Context, storespec.AuthorStamp) (accessdoor.AccessHandle, error) {
	return r.handle, nil
}

type gatedArms struct {
	pen    *recordingPen
	access *recordingResourceAccess
	state  *recordingAccess
	sched  *recordingSchedule
}

// newGatedActors starts one Server-hosted managed body over recording minters
// and returns the exact final Caps actorctl welded, plus the raw arms behind
// them. The pen is caller-supplied so a test can install the block-in-raw hook.
func newGatedActors(t *testing.T, pen *recordingPen) (*ChannelActors, actorcaps.Caps, gatedArms) {
	t.Helper()
	store := newFakeStore("agent")
	arms := gatedArms{
		pen:    pen,
		access: &recordingResourceAccess{},
		state:  &recordingAccess{},
		sched:  &recordingSchedule{},
	}
	var caps atomic.Pointer[actorcaps.Caps]
	actors, err := NewChannelActors(Config{
		Store:          store,
		ServerDomain:   "server",
		ServerHost:     actorhost.Config{PollInterval: time.Millisecond},
		ChannelID:      channel.ID("ch"),
		PenMinter:      fakePenMinter{pen: arms.pen},
		AccessMinter:   fakeAccessMinter{handle: arms.access},
		StateResolver:  fakeStateResolver{handle: arms.state},
		ScheduleMinter: fakeScheduleMinter{handle: arms.sched},
		BuildManagedBody: func(_ ManagedBodyInput, built actorcaps.Caps) actorrt.Actor {
			snapshot := built
			caps.Store(&snapshot)
			return inertActor{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := actors.Start(context.Background(), prepareSystem(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = actors.Close(ctx)
	})
	waitUntil(t, "managed body did not become current", func() bool {
		_, live := actors.Stat("agent")
		return live && caps.Load() != nil
	})
	return actors, *caps.Load(), arms
}

// stopDesiredReader freezes the sole Server desired reader so a white-box
// Controller mutation cannot be reconciled into the Host (which would retire the
// live G1 body). Mirrors TestServerCommandsOnlyWakeSingleDesiredReader.
func stopDesiredReader(actors *ChannelActors) {
	actors.serverDesiredCancel()
	actors.serverDesiredWG.Wait()
}

// publishNewAttempt turns the Controller value ledger over to a fresh attempt
// (G2) while leaving the physical Host body (G1) untouched — the exact window
// the value-ledger gate must close.
func publishNewAttempt(t *testing.T, actors *ChannelActors, id actor.ActorID) {
	t.Helper()
	key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	c := actors.controller
	c.stateMu.Lock()
	value := c.actors[id]
	value.Desired = DesiredState{AttemptKey: key}
	c.actors[id] = value
	c.stateMu.Unlock()
}

func TestManagedCapsRejectStaleBuildInsteadOfMixingSuccessorDefinition(t *testing.T) {
	actors, _, _ := newGatedActors(t, &recordingPen{})
	stopDesiredReader(actors)

	g1, ok, err := actors.controller.lookup("agent")
	if err != nil || !ok {
		t.Fatalf("lookup G1 = (%+v,%v,%v)", g1, ok, err)
	}
	oldInput := actorhost.BodyBuildInput{
		ActorID:       "agent",
		AttemptKey:    g1.Desired.AttemptKey,
		ExecutionSpec: g1.Definition.Execution,
	}
	mismatchedInput := oldInput
	mismatchedInput.ExecutionSpec.Config = []byte(`{"wrong":"same-attempt"}`)
	if _, err := actors.buildManagedCaps(mismatchedInput); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("same-attempt mismatched caps build error=%v, want ErrStaleAttempt", err)
	}

	g2Key, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	actors.controller.stateMu.Lock()
	g2 := actors.controller.actors["agent"]
	g2.Desired.AttemptKey = g2Key
	g2.Definition.Execution.Class = "successor-class"
	g2.Definition.Execution.Config = []byte(`{"generation":2}`)
	actors.controller.actors["agent"] = g2
	actors.controller.stateMu.Unlock()

	if _, err := actors.buildManagedCaps(oldInput); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale G1 caps build error=%v, want ErrStaleAttempt", err)
	}
}

func penOf(t *testing.T, caps actorcaps.Caps) currentPen {
	t.Helper()
	wrapped, ok := caps.Pen.(currentPen)
	if !ok {
		t.Fatalf("Pen is %T, want currentPen", caps.Pen)
	}
	return wrapped
}

// TestManagedGateRefusesStaleGenerationOnEveryArm covers the primary window:
// Controller published G2, Host actual is still G1, and every one of the five
// arms refuses a new call without touching its raw handle.
func TestManagedGateRefusesStaleGenerationOnEveryArm(t *testing.T) {
	actors, caps, arms := newGatedActors(t, &recordingPen{})
	stopDesiredReader(actors)
	publishNewAttempt(t, actors, "agent")

	ctx := context.Background()
	if _, err := caps.Pen.Write(ctx, &message.Envelope{}); err == nil {
		t.Fatal("Pen admitted a stale generation")
	}
	if _, err := caps.Access.Invoke(ctx, access.Operation(""), "", nil, nil); err == nil {
		t.Fatal("Access admitted a stale generation")
	}
	if _, err := caps.State.Invoke(ctx, access.Operation(""), "", nil, nil); err == nil {
		t.Fatal("State admitted a stale generation")
	}
	if _, err := caps.Schedule.Schedule(ctx, schedule.ScheduleReq{}); err == nil {
		t.Fatal("Schedule admitted a stale generation")
	}
	if err := caps.Lifecycle.EndSelf(ctx, actorcaps.EndSelfRequest{}); err == nil {
		t.Fatal("Lifecycle admitted a stale generation")
	}

	if got := arms.pen.writes.Load(); got != 0 {
		t.Fatalf("raw pen writes=%d, want 0", got)
	}
	if got := arms.access.calls.Load(); got != 0 {
		t.Fatalf("raw access calls=%d, want 0", got)
	}
	if got := arms.state.invokes.Load(); got != 0 {
		t.Fatalf("raw state invokes=%d, want 0", got)
	}
	if got := arms.sched.calls.Load(); got != 0 {
		t.Fatalf("raw schedule calls=%d, want 0", got)
	}
}

// TestManagedGatePassRunsOnceThenRefusesNextGeneration nails the sliding window:
// a call that clears the gate runs its raw arm exactly once; after G1→G2 the
// next call is refused at the ledger.
func TestManagedGatePassRunsOnceThenRefusesNextGeneration(t *testing.T) {
	actors, caps, arms := newGatedActors(t, &recordingPen{})
	ctx := context.Background()

	if _, err := caps.Pen.Write(ctx, &message.Envelope{}); err != nil {
		t.Fatalf("current generation Pen write rejected: %v", err)
	}
	if got := arms.pen.writes.Load(); got != 1 {
		t.Fatalf("raw pen writes=%d after one accepted call, want 1", got)
	}

	stopDesiredReader(actors)
	publishNewAttempt(t, actors, "agent")

	if _, err := caps.Pen.Write(ctx, &message.Envelope{}); err == nil {
		t.Fatal("Pen admitted a call after the ledger turned over")
	}
	if got := arms.pen.writes.Load(); got != 1 {
		t.Fatalf("raw pen writes=%d after the refused call, want 1", got)
	}
}

// TestManagedGateInFlightCallCompletesAfterTerminal pins the "allow" half of the
// sliding window against terminal: a call that already cleared the gate reaches
// its raw arm and completes once even though a terminal commits mid-flight — the
// gate never re-checks or waits — and the NEXT entry is refused.
func TestManagedGateInFlightCallCompletesAfterTerminal(t *testing.T) {
	pen := &recordingPen{entered: make(chan struct{}), release: make(chan struct{})}
	actors, caps, arms := newGatedActors(t, pen)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, err := caps.Pen.Write(ctx, &message.Envelope{})
		done <- err
	}()

	select {
	case <-pen.entered:
	case <-time.After(time.Second):
		t.Fatal("in-flight call never cleared the gate into raw")
	}

	if _, err := actors.End(ctx, EndRequest{Target: "agent"}); err != nil {
		t.Fatalf("terminal End failed: %v", err)
	}

	close(pen.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("in-flight call was retro-refused after gate pass: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight call did not complete")
	}
	if got := arms.pen.writes.Load(); got != 1 {
		t.Fatalf("raw pen writes=%d, want exactly 1", got)
	}

	if _, err := caps.Pen.Write(ctx, &message.Envelope{}); err == nil {
		t.Fatal("Pen admitted a new call after terminal")
	}
	if got := arms.pen.writes.Load(); got != 1 {
		t.Fatalf("raw pen writes=%d after post-terminal refusal, want 1", got)
	}
}

// TestManagedGateRefusesAllArmsAfterTerminal covers End's aftermath: the old
// Unit's four business arms all refuse a new call at the entry.
func TestManagedGateRefusesAllArmsAfterTerminal(t *testing.T) {
	actors, caps, arms := newGatedActors(t, &recordingPen{})
	ctx := context.Background()

	if _, err := actors.End(ctx, EndRequest{Target: "agent"}); err != nil {
		t.Fatalf("terminal End failed: %v", err)
	}

	if _, err := caps.Pen.Write(ctx, &message.Envelope{}); err == nil {
		t.Fatal("Pen admitted a call after terminal")
	}
	if _, err := caps.Access.Invoke(ctx, access.Operation(""), "", nil, nil); err == nil {
		t.Fatal("Access admitted a call after terminal")
	}
	if _, err := caps.State.Invoke(ctx, access.Operation(""), "", nil, nil); err == nil {
		t.Fatal("State admitted a call after terminal")
	}
	if _, err := caps.Schedule.Schedule(ctx, schedule.ScheduleReq{}); err == nil {
		t.Fatal("Schedule admitted a call after terminal")
	}
	if arms.pen.writes.Load()+arms.access.calls.Load()+arms.state.invokes.Load()+arms.sched.calls.Load() != 0 {
		t.Fatal("a raw arm ran after terminal")
	}
}

// TestManagedCapsShareOneGate is the pointer-identity acceptance: all five arms
// carry the exact same managedInvocation object.
func TestManagedCapsShareOneGate(t *testing.T) {
	_, caps, _ := newGatedActors(t, &recordingPen{})

	pen := penOf(t, caps)
	gate := pen.gate
	if gate == nil {
		t.Fatal("Pen gate is nil")
	}
	access, ok := caps.Access.(currentResourceAccess)
	if !ok || access.gate != gate {
		t.Fatal("Access does not share the Pen gate")
	}
	state, ok := caps.State.(currentAccess)
	if !ok || state.gate != gate {
		t.Fatal("State does not share the Pen gate")
	}
	sched, ok := caps.Schedule.(currentSchedule)
	if !ok || sched.gate != gate {
		t.Fatal("Schedule does not share the Pen gate")
	}
	life, ok := caps.Lifecycle.(managedLifecycle)
	if !ok || life.gate != gate {
		t.Fatal("Lifecycle does not share the Pen gate")
	}
}

// TestManagedGateAdmitTakesNoControlGate pins the lock discipline without
// reaching into Controller's private lock graph: a paused Restart owns the
// actor's semantic transition, while the pre-bound invocation probe remains a
// sliding-window snapshot read.
func TestManagedGateAdmitTakesNoControlGate(t *testing.T) {
	actors, caps, _ := newGatedActors(t, &recordingPen{})
	gate := penOf(t, caps).gate
	store := actors.controller.store.(*fakeStore)
	store.restartCommitted = make(chan struct{}, 1)
	store.restartResume = make(chan struct{})
	restarted := make(chan error, 1)
	go func() {
		restarted <- actors.Restart(context.Background(), RestartRequest{ActorID: "agent"})
	}()
	<-store.restartCommitted

	done := make(chan error, 1)
	go func() { done <- gate.admit() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("admit returned an error while healthy: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admit blocked on the per-actor controlGate")
	}
	close(store.restartResume)
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
}
