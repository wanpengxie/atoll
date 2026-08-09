package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

type testProvider struct {
	mu        sync.Mutex
	workers   []*testWorker
	neverReap bool
}

func (p *testProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: "test", Capabilities: driverproto.Capabilities{Interrupt: true, Steer: true}, Describe: introspect.Describe{Description: "test"}}
}
func (p *testProvider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	w := &testWorker{host: h, reaped: make(chan struct{}), neverReap: p.neverReap}
	p.mu.Lock()
	p.workers = append(p.workers, w)
	p.mu.Unlock()
	return w, nil
}

type testWorker struct {
	host      driverproto.WorkerHost
	reaped    chan struct{}
	once      sync.Once
	neverReap bool
}

func (w *testWorker) Open(context.Context, driverproto.OpenRequest) {
	w.host.Events().Publish(driverproto.WorkerReady{})
}
func (w *testWorker) Start(_ context.Context, r driverproto.StartRequest) {
	target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "turn"}
	w.host.Events().Publish(driverproto.TurnStarted{Target: target})
	w.host.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, FinalText: "done"})
}
func (w *testWorker) Control(_ context.Context, r driverproto.ControlRequest) {
	w.host.Events().Publish(driverproto.ControlOutcome{Action: r.Action, Target: r.Target, Verdict: driverproto.ControlAccepted})
}
func (w *testWorker) Retire() {
	if !w.neverReap {
		w.once.Do(func() { close(w.reaped) })
	}
}
func (w *testWorker) Reaped() <-chan struct{} { return w.reaped }

type resumeRetryProvider struct {
	mu     sync.Mutex
	next   int
	opened chan []byte
}

func (p *resumeRetryProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{
		Name:         "resume-retry",
		Capabilities: driverproto.Capabilities{Resume: true},
		Describe:     introspect.Describe{Description: "resume retry test"},
	}
}

func (p *resumeRetryProvider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	p.mu.Lock()
	index := p.next
	p.next++
	p.mu.Unlock()
	return &resumeRetryWorker{index: index, host: h, opened: p.opened, reaped: make(chan struct{})}, nil
}

type resumeRetryWorker struct {
	index  int
	host   driverproto.WorkerHost
	opened chan []byte
	reaped chan struct{}
	once   sync.Once
}

func (w *resumeRetryWorker) Open(_ context.Context, req driverproto.OpenRequest) {
	w.opened <- append([]byte(nil), req.ResumeSeed...)
	w.host.Events().Publish(driverproto.WorkerReady{})
}

func (w *resumeRetryWorker) Start(_ context.Context, req driverproto.StartRequest) {
	if w.index == 0 {
		w.host.Events().Publish(driverproto.SubmissionRejected{
			Attempt:     req.Attempt,
			Class:       driverproto.FailureResumeInvalid,
			Detail:      "stale seed",
			Disposition: driverproto.RetireWorker,
		})
		return
	}
	target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: "retried-turn"}
	w.host.Events().Publish(driverproto.TurnStarted{Target: target})
	w.host.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, FinalText: "retried"})
}

func (*resumeRetryWorker) Control(context.Context, driverproto.ControlRequest) {}
func (w *resumeRetryWorker) Retire()                                           { w.once.Do(func() { close(w.reaped) }) }
func (w *resumeRetryWorker) Reaped() <-chan struct{}                           { return w.reaped }

type collectedEvent struct {
	kind string
	op   runtimeproto.OpID
	turn runtimeproto.TurnID
	text string
}
type eventCollector struct{ ch chan collectedEvent }

func newCollector() *eventCollector { return &eventCollector{ch: make(chan collectedEvent, 32)} }
func (c *eventCollector) TurnStarted(op runtimeproto.OpID, id runtimeproto.TurnID) {
	c.ch <- collectedEvent{kind: "started", op: op, turn: id}
}
func (c *eventCollector) TurnRejected(op runtimeproto.OpID, code, detail string) {
	c.ch <- collectedEvent{kind: "rejected", op: op, text: code + detail}
}
func (c *eventCollector) Tool(runtimeproto.TurnID, runtimeproto.ToolEvent) {}
func (c *eventCollector) TurnEnded(id runtimeproto.TurnID, _ runtimeproto.TurnStatus, text, _ string) {
	c.ch <- collectedEvent{kind: "ended", turn: id, text: text}
}
func (c *eventCollector) ControlDone(op runtimeproto.OpID, id runtimeproto.TurnID, _ runtimeproto.ControlVerdict, _ string) {
	c.ch <- collectedEvent{kind: "control", op: op, turn: id}
}
func (c *eventCollector) ReadyDone(op runtimeproto.OpID, _ runtimeproto.ReadyResult) {
	c.ch <- collectedEvent{kind: "ready", op: op}
}
func (c *eventCollector) ProviderLost(id runtimeproto.TurnID, _ runtimeproto.LostCause, detail string) {
	c.ch <- collectedEvent{kind: "lost", turn: id, text: detail}
}
func (c *eventCollector) ResumeSeedUpdated([]byte) {}
func (c *eventCollector) RuntimeFault(code, detail string) {
	c.ch <- collectedEvent{kind: "fault", text: code + detail}
}

type immediateToolBridge struct{}

func (immediateToolBridge) Catalog() []runtimeproto.ToolSpec { return nil }
func (immediateToolBridge) Invoke(context.Context, effectcap.Scope, runtimeproto.ToolInvocation) runtimeproto.ToolResult {
	return runtimeproto.ToolResult{Text: "ok"}
}

func awaitKind(t *testing.T, c *eventCollector, kind string) collectedEvent {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case got := <-c.ch:
			if got.kind == kind {
				return got
			}
		case <-deadline:
			t.Fatalf("no %s event", kind)
		}
	}
}

func TestRuntimeFactsDriveTurnAndUUIDv7(t *testing.T) {
	p := &testProvider{}
	factory, _, err := Build(p, Policy{OpenFactDeadline: time.Second, StartFactDeadline: time.Second, Watchdog: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: ctx}, nil, events)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Messages: []runtimeproto.Input{{Text: "hello"}}}); err != nil {
		t.Fatal(err)
	}
	started := awaitKind(t, events, "started")
	parsed, err := uuid.Parse(string(started.turn))
	if err != nil || parsed.Version() != 7 {
		t.Fatalf("turn id=%q version=%v err=%v", started.turn, parsed.Version(), err)
	}
	ended := awaitKind(t, events, "ended")
	if ended.turn != started.turn || ended.text != "done" {
		t.Fatalf("ended=%+v started=%+v", ended, started)
	}
}

func TestSubmissionResumeInvalidRetriesOnceWithEmptySeed(t *testing.T) {
	p := &resumeRetryProvider{opened: make(chan []byte, 2)}
	factory, _, err := Build(p, Policy{
		OpenFactDeadline:  time.Second,
		StartFactDeadline: time.Second,
		ReapedDemand:      time.Second,
		Watchdog:          time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, []byte("stale-seed"), events)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Messages: []runtimeproto.Input{{Text: "hello"}}}); err != nil {
		t.Fatal(err)
	}

	for index, want := range [][]byte{[]byte("stale-seed"), nil} {
		select {
		case got := <-p.opened:
			if string(got) != string(want) {
				t.Fatalf("open %d seed=%q want %q", index, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("open %d did not occur", index)
		}
	}
	started := awaitKind(t, events, "started")
	ended := awaitKind(t, events, "ended")
	if ended.turn != started.turn || ended.text != "retried" {
		t.Fatalf("ended=%+v started=%+v", ended, started)
	}
	p.mu.Lock()
	workers := p.next
	p.mu.Unlock()
	if workers != 2 {
		t.Fatalf("workers=%d want 2", workers)
	}
}

func TestSequentialCallbacksHaveNoPerTurnSemanticQuota(t *testing.T) {
	policy := Policy{CallbackCapacity: 1, Watchdog: time.Hour}.normalized()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := driverproto.WorkerTurnTarget{Attempt: 1, Native: "turn"}
	e := &engine{
		policy:     policy,
		deps:       runtimeproto.Deps{Tools: immediateToolBridge{}},
		events:     newCollector(),
		root:       ctx,
		inbox:      newInbox(policy),
		timers:     map[timerKind]*time.Timer{},
		generation: generationState{id: 1, phase: generationRunning},
		turn:       &turnState{id: "canonical", target: target, life: ctx, callbacks: map[string]*callbackRow{}},
	}
	defer func() {
		for _, timer := range e.timers {
			timer.Stop()
		}
	}()

	for i := 0; i < 3; i++ {
		r := &callbackRequest{
			generation: 1,
			kind:       callbackTool,
			target:     target,
			callID:     fmt.Sprintf("call-%d", i),
			tool:       driverproto.ToolInvocation{Name: "test"},
			ctx:        ctx,
			response:   make(chan callbackResult, 1),
		}
		if !e.inbox.push(classCallback, r) {
			t.Fatalf("callback %d was rejected by an empty physical inbox", i)
		}
		entry, ok := e.inbox.pop()
		if !ok || entry.value != r {
			t.Fatalf("callback %d admission was not consumed", i)
		}
		e.handleCallback(r)

		deadline := time.After(time.Second)
		for {
			entry, ok := e.inbox.pop()
			if ok {
				completion, ok := entry.value.(callbackCompletion)
				if !ok {
					t.Fatalf("callback %d produced %T", i, entry.value)
				}
				e.handleCallbackCompletion(completion)
				break
			}
			select {
			case <-e.inbox.wake:
			case <-deadline:
				t.Fatalf("callback %d did not complete", i)
			}
		}
		select {
		case result := <-r.response:
			if result.tool.IsError {
				t.Fatalf("callback %d failed: %+v", i, result.tool)
			}
		default:
			t.Fatalf("callback %d result was not returned", i)
		}
	}
	if got := len(e.turn.callbacks); got != 3 {
		t.Fatalf("callback rows=%d want 3 distinct CallID rows", got)
	}
}

func TestBuildUsesPhysicalEventCapacityWithoutObservationBudget(t *testing.T) {
	_, spec, err := Build(&testProvider{}, Policy{IngressCapacity: 1, EventCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Bounds.EventCapacity != 1 {
		t.Fatalf("event capacity=%d want 1", spec.Bounds.EventCapacity)
	}
}

type controlCrashProvider struct {
	mu      sync.Mutex
	workers []*controlCrashWorker
}

func (p *controlCrashProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: "control-crash", Capabilities: driverproto.Capabilities{Interrupt: true}, Describe: introspect.Describe{Description: "control crash test"}}
}
func (p *controlCrashProvider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	w := &controlCrashWorker{host: h, reaped: make(chan struct{}), controlled: make(chan struct{})}
	p.mu.Lock()
	p.workers = append(p.workers, w)
	p.mu.Unlock()
	return w, nil
}

// controlCrashWorker starts a turn, swallows the control request without a
// verdict, and lets the test end the turn and crash the process.
type controlCrashWorker struct {
	host       driverproto.WorkerHost
	mu         sync.Mutex
	target     driverproto.WorkerTurnTarget
	reaped     chan struct{}
	controlled chan struct{}
	once       sync.Once
}

func (w *controlCrashWorker) Open(context.Context, driverproto.OpenRequest) {
	w.host.Events().Publish(driverproto.WorkerReady{})
}
func (w *controlCrashWorker) Start(_ context.Context, r driverproto.StartRequest) {
	target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "turn"}
	w.mu.Lock()
	w.target = target
	w.mu.Unlock()
	w.host.Events().Publish(driverproto.TurnStarted{Target: target})
}
func (w *controlCrashWorker) Control(context.Context, driverproto.ControlRequest) {
	close(w.controlled)
}
func (w *controlCrashWorker) endTurn() {
	w.mu.Lock()
	target := w.target
	w.mu.Unlock()
	w.host.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnInterrupted})
}
func (w *controlCrashWorker) crash()                  { w.once.Do(func() { close(w.reaped) }) }
func (w *controlCrashWorker) Retire()                 {}
func (w *controlCrashWorker) Reaped() <-chan struct{} { return w.reaped }

func TestUnexpectedReapSettlesPendingControlOnTerminalTurn(t *testing.T) {
	p := &controlCrashProvider{}
	factory, _, err := Build(p, Policy{
		OpenFactDeadline:    time.Second,
		StartFactDeadline:   time.Second,
		ControlFactDeadline: time.Minute,
		Watchdog:            time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, nil, events)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Messages: []runtimeproto.Input{{Text: "hello"}}}); err != nil {
		t.Fatal(err)
	}
	started := awaitKind(t, events, "started")
	if err := rt.Control(runtimeproto.ControlCommand{Op: 2, Target: started.turn, Kind: runtimeproto.ControlInterrupt}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	w := p.workers[0]
	p.mu.Unlock()
	select {
	case <-w.controlled:
	case <-time.After(time.Second):
		t.Fatal("control was not dispatched to the worker")
	}
	w.endTurn()
	awaitKind(t, events, "ended")
	w.crash()
	settled := awaitKind(t, events, "control")
	if settled.op != 2 || settled.turn != started.turn {
		t.Fatalf("control settled=%+v want op=2 turn=%s", settled, started.turn)
	}
}

func TestCloseDoesNotWaitForBrokenReaped(t *testing.T) {
	p := &testProvider{neverReap: true}
	factory, _, err := Build(p, Policy{OpenFactDeadline: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, nil, events)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.EnsureReady(1); err != nil {
		t.Fatal(err)
	}
	awaitKind(t, events, "ready")
	done := make(chan struct{})
	go func() { rt.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close waited for Reaped")
	}
}

var _ driverproto.Provider = (*testProvider)(nil)
var _ driverproto.Worker = (*testWorker)(nil)
var _ driverproto.Provider = (*resumeRetryProvider)(nil)
var _ driverproto.Worker = (*resumeRetryWorker)(nil)
var _ runtimeproto.Events = (*eventCollector)(nil)
