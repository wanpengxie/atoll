package base

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type testState struct {
	values map[resource.ResourceID][]byte
	put    func(resource.ResourceID, []byte) (accessdoor.Outcome, error)
}

func (s *testState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	v, ok := s.values[id]
	return accessdoor.Outcome{Value: v, Found: ok}, nil
}
func (s *testState) Put(id resource.ResourceID, v []byte) (accessdoor.Outcome, error) {
	if s.put != nil {
		return s.put(id, v)
	}
	s.values[id] = append([]byte(nil), v...)
	return accessdoor.Outcome{}, nil
}
func (s *testState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	delete(s.values, id)
	return accessdoor.Outcome{}, nil
}

type emptyPending struct{}

func (emptyPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return actorbase.Msg{}, nil
}
func (emptyPending) Cancel() error { return nil }

type testSys struct {
	self    actor.ActorID
	state   *testState
	life    context.Context
	emitErr error
	emits   []behavior.EventSpec
	terms   []testTerminal
	steps   []testProgress
	obs     []actorrt.ObsKind
	// order is the interleaved write sequence, so tests can assert the log's
	// shape in time (terminals before phase markers) and not just its contents.
	order []string
	// calls counts outbound Call attempts; callErrs makes that many of them
	// fail first, standing in for a link that is still coming up.
	calls    int
	callErrs int
}

type testTerminal struct {
	id    message.ID
	kind  string
	code  string
	value any
}

type testProgress struct {
	id     message.ID
	status string
}

func newTestSys() *testSys {
	return &testSys{self: "agent:test", state: &testState{values: map[resource.ResourceID][]byte{}}, life: context.Background()}
}
func (s *testSys) Reply(msg actorbase.Msg, value any) (message.ID, error) {
	s.terms = append(s.terms, testTerminal{id: msg.ID, kind: "reply", value: value})
	s.order = append(s.order, "reply:"+string(msg.ID))
	return "", nil
}
func (s *testSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	s.terms = append(s.terms, testTerminal{id: msg.ID, kind: "fail", code: code, value: detail})
	s.order = append(s.order, "fail:"+string(msg.ID))
	return "", nil
}
func (s *testSys) Progress(msg actorbase.Msg, status string, _ any) (message.ID, error) {
	s.steps = append(s.steps, testProgress{id: msg.ID, status: status})
	return "", nil
}
func (s *testSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.emits = append(s.emits, spec)
	s.order = append(s.order, "emit:"+spec.Type)
	return "", s.emitErr
}
func (*testSys) Post(behavior.RequestSpec) (message.ID, error) { return "", nil }
func (s *testSys) Call(actor.ActorID, string, any) (actorbase.Pending, error) {
	s.calls++
	if s.callErrs > 0 {
		s.callErrs--
		return nil, errors.New("compute: daemon outbound disconnected")
	}
	return emptyPending{}, nil
}
func (s *testSys) State() actorbase.StateHandle     { return s.state }
func (*testSys) Resource() actorbase.ResourceHandle { return nil }
func (*testSys) After(time.Duration, string, any, schedule.TimerHome) (schedule.TimerID, error) {
	return "", nil
}
func (*testSys) CancelTimer(schedule.TimerID) error                         { return nil }
func (*testSys) Fork(message.ID, actorcaps.ForkSpec) (actor.ActorID, error) { return "", nil }
func (*testSys) End() error                                                 { return nil }
func (s *testSys) PublishObs(kind actorrt.ObsKind, _ actorrt.ObsValue) error {
	s.obs = append(s.obs, kind)
	return nil
}
func (s *testSys) Self() actor.ActorID        { return s.self }
func (*testSys) Recv() (actorbase.Msg, error) { return actorbase.Msg{}, actorbase.ErrRecvDone }
func (s *testSys) Life() context.Context      { return s.life }

type testEngine struct {
	starts           int
	batches          [][]Trigger
	backgrounds      [][]ContextItem
	steers           []OpID
	interrupts       []OpID
	terminates       int
	ensures          []OpID
	terminateGate    <-chan struct{}
	terminateStarted chan<- struct{}
}

func (*testEngine) Boot(context.Context, BootPort) error { return nil }
func (e *testEngine) StartTurn(_ OpID, batch []Trigger, background []ContextItem) error {
	e.starts++
	e.batches = append(e.batches, append([]Trigger(nil), batch...))
	e.backgrounds = append(e.backgrounds, append([]ContextItem(nil), background...))
	return nil
}
func (e *testEngine) Steer(op OpID, _ Trigger) error { e.steers = append(e.steers, op); return nil }
func (e *testEngine) Interrupt(op OpID) error        { e.interrupts = append(e.interrupts, op); return nil }
func (e *testEngine) Terminate() error {
	e.terminates++
	if e.terminateStarted != nil {
		e.terminateStarted <- struct{}{}
	}
	if e.terminateGate != nil {
		<-e.terminateGate
	}
	return nil
}
func (e *testEngine) EnsureAlive(op OpID) error   { e.ensures = append(e.ensures, op); return nil }
func (*testEngine) Describe() introspect.Describe { return introspect.Describe{} }
func (*testEngine) Close() error                  { return nil }

func testMsg(kind message.Kind, sender actor.ActorID, typ string) actorbase.Msg {
	payload, _ := json.Marshal(map[string]any{"text": "hello"})
	env := message.Envelope{ID: "m1", Kind: kind, Type: typ, Sender: message.Sender{ID: sender}, Payload: payload, Visibility: message.VisibilityPublic}
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)
}

func newUnitLoop() (*agentLoop, *testEngine) {
	sys := newTestSys()
	eng := &testEngine{}
	d := definition{cfg: Config{BufferMaxCount: 8, BufferMaxBytes: 1024, BatchMaxCount: 4}, controls: map[string]struct{}{TypeQueue: {}, TypeStop: {}, TypeSteer: {}}}
	return &agentLoop{def: d, sys: sys, eng: eng, events: make(chan providerEvent, 16), state: stateIdle, buffer: requestBuffer{maxCount: 8, maxBytes: 1024}, committing: map[OpID]*operation{}, controlExpiry: make(chan *controlSlot, 4)}, eng
}

func awaitEngineAction(t *testing.T, l *agentLoop) providerEvent {
	t.Helper()
	select {
	case event := <-l.events:
		l.handleProviderEvent(event)
		return event
	case <-time.After(time.Second):
		t.Fatal("engine action did not complete")
		return providerEvent{}
	}
}

func TestSelfEmissionIgnored(t *testing.T) {
	l, e := newUnitLoop()
	l.handleIntake(testMsg(message.KindRequest, "agent:test", "user.text"), make(chan closureEvent, 1))
	if e.starts != 0 {
		t.Fatalf("self request started %d turns", e.starts)
	}
}

func TestOtherAgentActivityAndFinalResponseAreContextOnly(t *testing.T) {
	l, e := newUnitLoop()
	ch := make(chan closureEvent, 2)
	l.handleIntake(testMsg(message.KindEvent, "agent:other", "activity.turn.started"), ch)
	l.handleIntake(testMsg(message.KindResponse, "agent:other", "answer"), ch)
	if e.starts != 0 {
		t.Fatalf("context messages started %d turns", e.starts)
	}
}

func TestIgnoredMailboxTrafficDoesNotRefreshActiveWatchdog(t *testing.T) {
	l, _ := newUnitLoop()
	l.state = stateTurnActive
	closures := make(chan closureEvent, 1)
	// A short lease stands in for a nearly-spent watchdog: if any ignored
	// traffic refreshed it, the reconcile would stretch it to the full 10min
	// and the timer would never fire inside this test.
	watchdog := time.NewTimer(50 * time.Millisecond)
	for _, msg := range []actorbase.Msg{
		testMsg(message.KindEvent, "agent:other", "activity.turn.started"),
		testMsg(message.KindResponse, "agent:other", "answer"),
		testMsg(message.KindRequest, "agent:test", "user.text"),
	} {
		before := l.turnIndex
		l.handleIntake(msg, closures)
		l.reconcileWatchdog(watchdog, false, before)
	}
	select {
	case <-watchdog.C:
	case <-time.After(2 * time.Second):
		t.Fatal("ignored mailbox traffic refreshed the provider watchdog")
	}
}

func TestCrashRecoveryTurnTakesFreshWatchdogLease(t *testing.T) {
	l, e := newUnitLoop()
	l.enqueue(bufferedMsg("first", "actor:a", 1), false)
	l.handleProviderEvent(providerEvent{kind: eventTurnStarted, op: OpID("op-1"), turnID: "t1"})
	if l.state != stateTurnActive || e.starts != 1 {
		t.Fatalf("setup state=%v starts=%d", l.state, e.starts)
	}
	l.buffer.push(bufferedMsg("queued", "actor:a", 2))
	// The old turn's lease is nearly spent when the provider crashes. The
	// recovery turn started inside providerLost must take a FRESH lease: a
	// stale remainder would kill a healthy new turn within seconds.
	watchdog := time.NewTimer(50 * time.Millisecond)
	turnBefore := l.turnIndex
	l.providerLost(LostCrash, "boom")
	if e.starts != 2 {
		t.Fatalf("recovery did not start the queued turn: starts=%d", e.starts)
	}
	l.reconcileWatchdog(watchdog, false, turnBefore)
	select {
	case <-watchdog.C:
		t.Fatal("recovered turn inherited the crashed turn's stale watchdog lease")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestStartingBuffersWithoutSecondStartTurn(t *testing.T) {
	l, e := newUnitLoop()
	l.enqueue(bufferedMsg("first", "actor:a", 1), false)
	if l.state != stateStarting || e.starts != 1 {
		t.Fatalf("first state=%v starts=%d", l.state, e.starts)
	}
	l.enqueue(bufferedMsg("second", "actor:a", 1), true)
	if e.starts != 1 || len(l.buffer.items) != 1 {
		t.Fatalf("second caused ghost start: starts=%d buffer=%#v", e.starts, l.buffer.items)
	}
}

func TestReservedTypesMechanicalAndOpenRemainderGoesToEngine(t *testing.T) {
	l, e := newUnitLoop()
	ch := make(chan closureEvent, 4)
	l.handleIntake(testMsg(message.KindRequest, "actor:user", "agent.future"), ch)
	if terms := l.sys.(*testSys).terms; len(terms) != 1 || terms[0].code != "type_unsupported" || e.starts != 0 {
		t.Fatalf("reserved result terms=%#v starts=%d", terms, e.starts)
	}
	l.handleIntake(testMsg(message.KindRequest, "actor:user", "vendor.anything"), ch)
	if e.starts != 1 {
		t.Fatalf("open type starts=%d", e.starts)
	}
}

func TestUnsupportedSteerDegradesToQueueInsteadOfFailing(t *testing.T) {
	l, engine := newUnitLoop()
	delete(l.def.controls, TypeSteer)
	l.state, l.turnID = stateTurnActive, "turn"
	l.handleIntake(testMsg(message.KindRequest, "actor:user", TypeSteer), make(chan closureEvent, 1))
	if len(l.sys.(*testSys).terms) != 0 {
		t.Fatalf("unsupported steer was failed: %#v", l.sys.(*testSys).terms)
	}
	if len(l.buffer.items) != 1 || len(engine.steers) != 0 {
		t.Fatalf("steer did not degrade to queue: buffer=%#v steers=%v", l.buffer.items, engine.steers)
	}
}

var _ actorbase.Sys = (*testSys)(nil)
var _ Engine = (*testEngine)(nil)
