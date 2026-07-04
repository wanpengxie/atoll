package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// --- fakes: a stub Caps built entirely from this package + protocol/runtime
// leaf vocabulary — zero platform dependency (spec DoD⑥). ------------------

// fakePen is a harness.Pen double: it welds a fixed self identity onto every
// written envelope (mirroring boundPen's fail-fast weld) and records every
// write for assertions.
type fakePen struct {
	mu      sync.Mutex
	self    actor.ActorID
	written []*message.Envelope
	reject  harness.HarnessRejectReason // when set, every Write is rejected with this reason
}

func (p *fakePen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reject != "" {
		return harness.WriteResult{RejectReason: p.reject}, nil
	}
	env.Sender.ID = p.self
	p.written = append(p.written, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (p *fakePen) last() *message.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.written) == 0 {
		return nil
	}
	return p.written[len(p.written)-1]
}

func (p *fakePen) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.written)
}

// fakeAccess is an accessdoor.AccessHandle double never meaningfully
// exercised by these tests (State()/Resource() are thin pass-throughs;
// covering the pass-through shape is enough here).
type fakeAccess struct{}

func (fakeAccess) Invoke(_ context.Context, op access.Operation, id resource.ResourceID, args []byte, _ *access.Grant) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{Value: args}, nil
}

// fakeSchedule is a schedule.ScheduleHandle double: Schedule/Cancel are
// recorded, never actually fired (the engine's own timers are what these
// tests exercise, not the schedule arm).
type fakeSchedule struct{}

func (fakeSchedule) Schedule(_ context.Context, _ schedule.ScheduleReq) (schedule.TimerID, error) {
	return "timer-1", nil
}
func (fakeSchedule) Cancel(_ context.Context, _ schedule.TimerID) error { return nil }

// fakeSpawn is an actorrt.SpawnHandle double.
type fakeSpawn struct{}

func (fakeSpawn) Fork(spec actorrt.ForkSpec) (actor.ActorID, error) {
	return actor.ActorID("child/" + spec.NameHint), nil
}
func (fakeSpawn) Despawn(actor.ActorID) error { return nil }

// fakeActorContext is an actorrt.ActorContext double.
type fakeActorContext struct {
	self actor.ActorID

	mu   sync.Mutex
	pubs []struct {
		kind actorrt.ObsKind
		val  actorrt.ObsValue
	}
}

func (f *fakeActorContext) Self() actor.ActorID { return f.self }
func (f *fakeActorContext) PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, struct {
		kind actorrt.ObsKind
		val  actorrt.ObsValue
	}{kind, val})
}
func (f *fakeActorContext) obsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pubs)
}

// newTestEngine builds an *engine directly (bypassing New) so tests can
// shrink the ledger/queue capacities and inject a deterministic clock —
// same-package whitebox construction, zero platform import.
func newTestEngine(t *testing.T, pen *fakePen, hooks Hooks, serveCap, queueCap int) *engine {
	t.Helper()
	e := &engine{
		pen:      pen,
		access:   fakeAccess{},
		state:    fakeAccess{},
		sched:    fakeSchedule{},
		spawn:    fakeSpawn{},
		hooks:    hooks,
		clockFn:  time.Now,
		queueCap: queueCap,
	}
	e.serve = newServeLedger(e.life, serveCap)
	e.call = newCallLedger(e.life, e.pen, e.clockFn, hooks)
	e.workQ = make(chan *message.Envelope, queueCap)
	e.rejectQ = make(chan *message.Envelope, queueCap)
	e.rejectStop = make(chan struct{})
	return e
}

func newRequestEnv(id message.ID, expiresInMs int64) *message.Envelope {
	env := &message.Envelope{ID: id, Kind: message.KindRequest, Type: "test.req"}
	if expiresInMs >= 0 {
		exp := time.Now().Add(time.Duration(expiresInMs) * time.Millisecond).UnixMilli()
		env.ExpiresAt = &exp
	}
	return env
}

// --- serve ledger: deadline-close and late-write judgement -----------------

func TestServeLedger_DeadlineFiresThenLedgerEmpty(t *testing.T) {
	life := func() context.Context { return context.Background() }
	l := newServeLedger(life, 8)
	env := newRequestEnv("req-1", 20)
	if !l.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	if l.len() != 1 {
		t.Fatalf("expected 1 admitted entry, got %d", l.len())
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for l.len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if l.len() != 0 {
		t.Fatalf("expected ledger empty after deadline, got len=%d", l.len())
	}
	if !l.isClosed(env.ID) {
		t.Fatal("expected entry closed after deadline")
	}
}

func TestEngine_LateReplyAfterDeadlineIsErrRequestClosed(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	env := newRequestEnv("req-2", 20)
	if !e.serve.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	ctx, _ := e.serve.ctxFor(env.ID)
	msg := NewMsg(ctx, *env)

	deadline := time.Now().Add(500 * time.Millisecond)
	for e.serve.len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	_, err := e.Reply(msg, map[string]string{"ok": "true"})
	if !errors.Is(err, ErrRequestClosed) {
		t.Fatalf("expected ErrRequestClosed, got %v", err)
	}
	if pen.count() != 0 {
		t.Fatalf("expected no write to land for a closed entry, got %d writes", pen.count())
	}
}

func TestEngine_ReplyClosesEntry(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	env := newRequestEnv("req-3", -1)
	e.serve.admit(env)
	ctx, _ := e.serve.ctxFor(env.ID)
	msg := NewMsg(ctx, *env)

	if _, err := e.Reply(msg, map[string]string{"greeting": "hello"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.serve.len() != 0 {
		t.Fatalf("expected entry closed after Reply, got len=%d", e.serve.len())
	}
	if pen.count() != 1 {
		t.Fatalf("expected exactly one write, got %d", pen.count())
	}
	// A second Reply against the now-closed entry is late.
	if _, err := e.Reply(msg, "again"); !errors.Is(err, ErrRequestClosed) {
		t.Fatalf("expected ErrRequestClosed on second Reply, got %v", err)
	}
}

// --- call ledger / Sys.Call / JobTable: await_result and Wait share one
// account -------------------------------------------------------------------

func responseEnv(parentID message.ID, status string) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": status})
	return &message.Envelope{
		ID:       message.ID("resp-" + parentID),
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  payload,
	}
}

func TestEngine_CallWaitReceivesMatchedFinal(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	pending, err := e.Call("actor:callee", "greet", map[string]string{"hi": "1"})
	if err != nil {
		t.Fatalf("unexpected Call error: %v", err)
	}
	sent := pen.last()
	if sent == nil {
		t.Fatal("expected a written outbound request")
	}

	done := make(chan struct{})
	var gotMsg Msg
	var gotErr error
	go func() {
		defer close(done)
		gotMsg, gotErr = pending.Wait(context.Background(), 2*time.Second)
	}()
	time.Sleep(20 * time.Millisecond)

	e.call.match(responseEnv(sent.ID, message.StatusCompleted))
	<-done

	if gotErr != nil {
		t.Fatalf("unexpected Wait error: %v", gotErr)
	}
	if gotMsg.ParentID != sent.ID {
		t.Fatalf("expected matched response, got %+v", gotMsg)
	}
}

func TestEngine_JobTableAwaitSameAccountAsWait(t *testing.T) {
	pen := &fakePen{self: "actor:caller"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	id, err := e.Submit(behavior.RequestSpec{
		Type:     "greet",
		Audience: message.Audience{"actor:callee"},
	})
	if err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}

	// Final lands BEFORE Await parks — must not be stranded (buffered-final
	// race, spec's reconcile guarantee).
	e.call.match(responseEnv(id, message.StatusCompleted))

	env, ok, err := e.Await(context.Background(), id, time.Second)
	if err != nil {
		t.Fatalf("unexpected Await error: %v", err)
	}
	if !ok || env == nil {
		t.Fatalf("expected buffered final delivered, got ok=%v env=%v", ok, env)
	}
}

// --- reject lane: overflow gets an overloaded terminal; a full reject lane
// leaves it to the caller's own author#2 ------------------------------------

func TestEngine_ServeLedgerFullRoutesToRejectLane(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 1 /*serveCap*/, 8)
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantRunning))
	go e.runRejectLane()
	defer close(e.rejectStop)

	// Fill the one serve-ledger slot.
	if err := e.Receive(context.Background(), newRequestEnv("req-a", -1)); err != nil {
		t.Fatalf("unexpected Receive error: %v", err)
	}
	// This one has no room to Admit — must route to the reject lane.
	if err := e.Receive(context.Background(), newRequestEnv("req-b", -1)); err != nil {
		t.Fatalf("unexpected Receive error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for pen.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	last := pen.last()
	if last == nil {
		t.Fatal("expected the reject lane to write an overloaded terminal")
	}
	var payload struct {
		ErrorCode string `json:"error_code"`
	}
	_ = json.Unmarshal(last.Payload, &payload)
	if payload.ErrorCode != "overloaded" {
		t.Fatalf("expected error_code=overloaded, got %+v (payload=%s)", payload, last.Payload)
	}
}

func TestEngine_RejectLaneFullDropsAndObsRecords(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	actx := &fakeActorContext{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 0 /*serveCap: nothing ever Admits*/, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = actx
	e.occupant.Store(int32(occupantRunning))
	// Reject lane goroutine deliberately NOT started: rejectQ (cap=8) plus one
	// more overflows without anyone ever draining it.
	for i := 0; i < 9; i++ {
		if err := e.Receive(context.Background(), newRequestEnv(message.ID(time.Now().Format(time.RFC3339Nano)+string(rune(i))), -1)); err != nil {
			t.Fatalf("unexpected Receive error: %v", err)
		}
	}
	if actx.obsCount() == 0 {
		t.Fatal("expected an obs record for the reject-lane overflow")
	}
}

// --- occupant arc: death broadcast timing + quiet/loud raw exit code -------

func TestEngine_DyingReportsQuietOnNilReturn(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actx := &fakeActorContext{self: "actor:test"}

	def := Def{New: func() (Proc, error) {
		return func(sys Sys) error { return nil }, nil
	}}
	e.def = def
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}

	select {
	case err := <-e.Dying():
		if err != nil {
			t.Fatalf("expected quiet (nil) exit, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Dying()")
	}
}

func TestEngine_DyingReportsLoudOnErrorReturn(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actx := &fakeActorContext{self: "actor:test"}

	boom := errors.New("boom")
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error { return boom }, nil
	}}
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}

	select {
	case err := <-e.Dying():
		if !errors.Is(err, boom) {
			t.Fatalf("expected loud exit %v, got %v", boom, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Dying()")
	}
}

// --- New: the caps→Sys assembly seam itself ---------------------------------

func TestNew_WeldsCapsIntoALiveActor(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	caps := actorcaps.Caps{
		Pen:      pen,
		Access:   fakeAccess{},
		State:    fakeAccess{},
		Schedule: fakeSchedule{},
		Spawn:    fakeSpawn{},
	}
	done := make(chan struct{})
	a := New(caps, Hooks{}, Def{New: func() (Proc, error) {
		return func(sys Sys) error {
			close(done)
			return nil
		}, nil
	}})

	starter, ok := a.(actorrt.Starter)
	if !ok {
		t.Fatal("expected New's result to implement actorrt.Starter")
	}
	if err := starter.Start(context.Background(), &fakeActorContext{self: "actor:test"}); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the Proc to run")
	}
}

func TestEngine_StopDrainsWorkerBeforeReturning(t *testing.T) {
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	ctx, cancel := context.WithCancel(context.Background())
	actx := &fakeActorContext{self: "actor:test"}

	started := make(chan struct{})
	e.def = Def{New: func() (Proc, error) {
		return func(sys Sys) error {
			close(started)
			<-sys.Life().Done()
			return nil
		}, nil
	}}
	if err := e.Start(ctx, actx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	<-started
	cancel() // simulate cell teardown cancelling the process-life ctx

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Stop(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the worker drained")
	}
}
