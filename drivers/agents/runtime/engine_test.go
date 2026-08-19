package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type testProvider struct {
	mu        sync.Mutex
	workers   []*testWorker
	neverReap bool
}

func (p *testProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: "test", Capabilities: map[string]bool{driverproto.CapabilityInterrupt: true, driverproto.CapabilitySteer: true}, Documentation: driverproto.Documentation{Description: "test"}}
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
	mu                sync.Mutex
	next              int
	opened            chan []byte
	rejectPhase       string
	succeedAfterRetry bool
}

func (p *resumeRetryProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{
		Name:          "resume-retry",
		Capabilities:  map[string]bool{driverproto.CapabilityResume: true},
		Documentation: driverproto.Documentation{Description: "resume retry test"},
	}
}

func (p *resumeRetryProvider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	p.mu.Lock()
	index := p.next
	p.next++
	p.mu.Unlock()
	return &resumeRetryWorker{index: index, host: h, opened: p.opened, rejectPhase: p.rejectPhase, succeedAfterRetry: p.succeedAfterRetry, reaped: make(chan struct{})}, nil
}

type resumeRetryWorker struct {
	index             int
	host              driverproto.WorkerHost
	opened            chan []byte
	rejectPhase       string
	succeedAfterRetry bool
	reaped            chan struct{}
	once              sync.Once
}

func (w *resumeRetryWorker) Open(_ context.Context, req driverproto.OpenRequest) {
	w.opened <- append([]byte(nil), req.ResumeSeed...)
	if w.rejectPhase == "open" && (w.index == 0 || !w.succeedAfterRetry) {
		w.host.Events().Publish(driverproto.OpenRejected{
			Class:       driverproto.FailureResumeInvalid,
			Detail:      "stale seed",
			Disposition: driverproto.RetireWorker,
		})
		return
	}
	w.host.Events().Publish(driverproto.WorkerReady{})
}

func (w *resumeRetryWorker) Start(_ context.Context, req driverproto.StartRequest) {
	if w.rejectPhase == "submission" && (w.index == 0 || !w.succeedAfterRetry) {
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
	kind  string
	op    runtimeproto.OpID
	turn  runtimeproto.TurnID
	text  string
	usage runtimeproto.TurnUsage
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
func (c *eventCollector) TurnEnded(id runtimeproto.TurnID, _ runtimeproto.TurnStatus, text, _ string, usage runtimeproto.TurnUsage) {
	c.ch <- collectedEvent{kind: "ended", turn: id, text: text, usage: usage}
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
	rt, err := factory(runtimeproto.Deps{Parent: ctx}, nil, runtimeproto.TurnOptions{}, events)
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

func TestResumeInvalidRetriesAtMostOnce(t *testing.T) {
	for _, tc := range []struct {
		name              string
		rejectPhase       string
		succeedAfterRetry bool
		wantFinal         string
	}{
		{name: "open_then_success", rejectPhase: "open", succeedAfterRetry: true, wantFinal: "ended"},
		{name: "open_twice", rejectPhase: "open", wantFinal: "rejected"},
		{name: "submission_then_success", rejectPhase: "submission", succeedAfterRetry: true, wantFinal: "ended"},
		{name: "submission_twice", rejectPhase: "submission", wantFinal: "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &resumeRetryProvider{opened: make(chan []byte, 3), rejectPhase: tc.rejectPhase, succeedAfterRetry: tc.succeedAfterRetry}
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
			rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, []byte("stale-seed"), runtimeproto.TurnOptions{}, events)
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
			final := awaitKind(t, events, tc.wantFinal)
			if tc.wantFinal == "ended" && final.text != "retried" {
				t.Fatalf("final=%+v want retried turn", final)
			}
			select {
			case seed := <-p.opened:
				t.Fatalf("resume retried more than once, third seed=%q", seed)
			case <-time.After(50 * time.Millisecond):
			}
			p.mu.Lock()
			workers := p.next
			p.mu.Unlock()
			if workers != 2 {
				t.Fatalf("workers=%d want 2", workers)
			}
		})
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
	return driverproto.ProviderSpec{Name: "control-crash", Capabilities: map[string]bool{driverproto.CapabilityInterrupt: true}, Documentation: driverproto.Documentation{Description: "control crash test"}}
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
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, nil, runtimeproto.TurnOptions{}, events)
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

func newStateEngine(t *testing.T) (*engine, *testWorker, *eventCollector) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	policy := Policy{
		OpenFactDeadline:    time.Hour,
		StartFactDeadline:   time.Hour,
		ControlFactDeadline: time.Hour,
		InterruptEnded:      time.Hour,
		Watchdog:            time.Hour,
		ReapedDemand:        time.Hour,
	}.normalized()
	provider := &testProvider{neverReap: true}
	events := newCollector()
	q := newInbox(policy)
	gate := &hostAdmission{}
	sink := &generationSink{generation: 1, queue: q, gate: gate, logger: slog.New(slog.DiscardHandler)}
	e := &engine{
		provider:     provider,
		providerSpec: provider.Spec(),
		policy:       policy,
		deps:         runtimeproto.Deps{Logger: slog.New(slog.DiscardHandler)},
		events:       events,
		root:         ctx,
		cancel:       cancel,
		inbox:        q,
		timers:       map[timerKind]*time.Timer{},
		generation:   generationState{id: 1, phase: generationReady, sink: sink},
	}
	if generation, ok := e.ids.Generation(); !ok || generation != 1 {
		t.Fatalf("initial generation=%d ok=%v", generation, ok)
	}
	_, workerCancel := context.WithCancel(ctx)
	w := &testWorker{reaped: make(chan struct{}), neverReap: true}
	if !e.slot.install(1, w, workerCancel) {
		t.Fatal("worker fixture install failed")
	}
	t.Cleanup(func() {
		for _, timer := range e.timers {
			timer.Stop()
		}
		e.slot.close()
		cancel()
	})
	return e, w, events
}

func setFixtureTurn(e *engine, starting, terminal bool) (*turnState, driverproto.WorkerTurnTarget) {
	target := driverproto.WorkerTurnTarget{Attempt: 1, Native: "native-turn"}
	life, cancel := context.WithCancel(e.root)
	t := &turnState{
		op:        11,
		attempt:   1,
		target:    target,
		id:        "canonical-turn",
		starting:  starting,
		terminal:  terminal,
		life:      life,
		cancel:    cancel,
		callbacks: map[string]*callbackRow{},
	}
	e.turn = t
	e.generation.phase = generationRunning
	return t, target
}

func drainEventKinds(c *eventCollector) []string {
	var kinds []string
	for {
		select {
		case event := <-c.ch:
			kinds = append(kinds, event.kind)
		default:
			return kinds
		}
	}
}

func TestReapedSettlementMatrix(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setup      func(*engine)
		wantEvents []string
		wantCarry  bool
	}{
		{name: "ready_empty", setup: func(e *engine) { e.generation.phase = generationReady }},
		{name: "retiring_empty", setup: func(e *engine) { e.generation.phase = generationRetiring }},
		{name: "opening_pending", setup: func(e *engine) {
			e.generation.phase = generationOpening
			e.pending = &pendingDemand{kind: demandReady, op: 9}
		}, wantCarry: true},
		{name: "starting", setup: func(e *engine) { setFixtureTurn(e, true, false) }, wantEvents: []string{"rejected"}},
		{name: "active", setup: func(e *engine) { setFixtureTurn(e, false, false) }, wantEvents: []string{"lost"}},
		{name: "active_with_pending", setup: func(e *engine) {
			setFixtureTurn(e, false, false)
			e.pending = &pendingDemand{kind: demandReady, op: 9}
		}, wantEvents: []string{"lost"}, wantCarry: true},
		{name: "active_with_control", setup: func(e *engine) {
			t, target := setFixtureTurn(e, false, false)
			t.control = &controlState{op: 12, action: 2, target: target}
		}, wantEvents: []string{"control", "lost"}},
		{name: "terminal_with_control", setup: func(e *engine) {
			t, target := setFixtureTurn(e, false, true)
			t.control = &controlState{op: 12, action: 2, target: target}
		}, wantEvents: []string{"control"}},
		{name: "retiring_terminal_with_control", setup: func(e *engine) {
			t, target := setFixtureTurn(e, false, true)
			t.control = &controlState{op: 12, action: 2, target: target}
			e.generation.phase = generationRetiring
		}, wantEvents: []string{"control"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, worker, events := newStateEngine(t)
			oldSink := e.generation.sink
			tc.setup(e)
			e.handleReaped(reapedFact{generation: 1, worker: worker})
			if got := drainEventKinds(events); !reflect.DeepEqual(got, tc.wantEvents) {
				t.Fatalf("events=%v want %v", got, tc.wantEvents)
			}
			if !oldSink.gate.sealed.Load() {
				t.Fatal("reaped generation sink remained open")
			}
			if e.turn != nil {
				t.Fatalf("turn survived generation wipe: %+v", e.turn)
			}
			if tc.wantCarry {
				if e.pending == nil || e.generation.id != 2 || e.generation.phase != generationOpening {
					t.Fatalf("pending demand was not carried to a fresh generation: pending=%+v generation=%+v", e.pending, e.generation)
				}
			} else if e.generation.id != 0 || e.generation.phase != generationNil || e.generation.sink != nil {
				t.Fatalf("generation was not wiped: %+v", e.generation)
			}
		})
	}
}

func TestGenerationWipeRefusesOutstandingDebt(t *testing.T) {
	e, _, events := newStateEngine(t)
	setFixtureTurn(e, false, false)
	if e.wipeSettledGeneration() {
		t.Fatal("generation wipe accepted a non-terminal turn")
	}
	if e.generation.id != 1 || e.turn == nil {
		t.Fatal("failed wipe mutated live generation state")
	}
	if got := drainEventKinds(events); !reflect.DeepEqual(got, []string{"fault"}) {
		t.Fatalf("events=%v want [fault]", got)
	}
}

func TestControlAndTerminalArrivalOrdersConverge(t *testing.T) {
	for _, tc := range []struct {
		name          string
		terminalFirst bool
		wantEvents    []string
	}{
		{name: "control_then_terminal", wantEvents: []string{"control", "ended"}},
		{name: "terminal_then_control", terminalFirst: true, wantEvents: []string{"ended", "control"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, events := newStateEngine(t)
			turn, target := setFixtureTurn(e, false, false)
			turn.control = &controlState{op: 12, action: 2, kind: runtimeproto.ControlSteer, target: target}
			outcome := driverproto.ControlOutcome{Action: 2, Target: target, Verdict: driverproto.ControlAccepted}
			terminal := driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, FinalText: "done"}
			if tc.terminalFirst {
				e.turnEnded(terminal)
				e.controlOutcome(outcome)
			} else {
				e.controlOutcome(outcome)
				e.turnEnded(terminal)
			}
			if got := drainEventKinds(events); !reflect.DeepEqual(got, tc.wantEvents) {
				t.Fatalf("events=%v want %v", got, tc.wantEvents)
			}
			if e.turn != nil || e.generation.phase != generationReady {
				t.Fatalf("order did not converge to reusable worker: turn=%+v generation=%+v", e.turn, e.generation)
			}
		})
	}
}

func TestCallbackAndTerminalArrivalOrders(t *testing.T) {
	for _, tc := range []struct {
		name            string
		terminalFirst   bool
		wantEvent       string
		wantResultError bool
	}{
		{name: "completion_then_terminal", wantEvent: "ended"},
		{name: "terminal_then_completion", terminalFirst: true, wantEvent: "lost", wantResultError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, worker, events := newStateEngine(t)
			turn, target := setFixtureTurn(e, false, false)
			request := &callbackRequest{generation: 1, kind: callbackTool, target: target, callID: "call", ctx: e.root, response: make(chan callbackResult, 1)}
			turn.callbacks["callback"] = &callbackRow{request: request, running: true}
			completion := callbackCompletion{generation: 1, key: "callback", result: callbackResult{tool: driverproto.ToolResult{Text: "ok"}}}
			terminal := driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, FinalText: "done"}
			if tc.terminalFirst {
				e.turnEnded(terminal)
				e.handleCallbackCompletion(completion)
			} else {
				e.handleCallbackCompletion(completion)
				e.turnEnded(terminal)
			}
			result := <-request.response
			if result.tool.IsError != tc.wantResultError {
				t.Fatalf("result=%+v want error=%v", result.tool, tc.wantResultError)
			}
			if got := drainEventKinds(events); !reflect.DeepEqual(got, []string{tc.wantEvent}) {
				t.Fatalf("events=%v want [%s]", got, tc.wantEvent)
			}
			if tc.terminalFirst {
				e.handleReaped(reapedFact{generation: 1, worker: worker})
				if e.generation.id != 0 || e.generation.phase != generationNil || e.generation.sink != nil || e.turn != nil {
					t.Fatalf("settled callback survived reap: generation=%+v turn=%+v", e.generation, e.turn)
				}
			} else if e.turn != nil || e.generation.phase != generationReady {
				t.Fatalf("completed callback did not leave reusable worker: turn=%+v generation=%+v", e.turn, e.generation)
			}
		})
	}
}

func TestStaleTimersCannotSettleCurrentTurnOrControl(t *testing.T) {
	e, _, events := newStateEngine(t)
	turn, target := setFixtureTurn(e, false, false)
	turn.watchdogRevision = 5
	turn.control = &controlState{op: 12, action: 2, target: target, revision: 7}
	e.handleTimer(timerFact{kind: timerWatchdog, generation: 1, revision: 4, attempt: 1})
	e.handleTimer(timerFact{kind: timerControl, generation: 1, revision: 6, action: 2})
	if got := drainEventKinds(events); len(got) != 0 {
		t.Fatalf("stale timers emitted events: %v", got)
	}
	if turn.terminal || turn.control == nil || e.generation.phase != generationRunning {
		t.Fatalf("stale timers mutated current state: turn=%+v generation=%+v", turn, e.generation)
	}
}

func TestCloseDoesNotWaitForBrokenReaped(t *testing.T) {
	p := &testProvider{neverReap: true}
	factory, _, err := Build(p, Policy{OpenFactDeadline: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, nil, runtimeproto.TurnOptions{}, events)
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
