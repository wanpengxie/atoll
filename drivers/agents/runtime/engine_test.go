package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type fakeAdapter struct {
	mu      sync.Mutex
	workers []*fakeWorker
	factory func(int, driverproto.WorkerHost) *fakeWorker
}

func (a *fakeAdapter) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w := a.factory(len(a.workers), h)
	a.workers = append(a.workers, w)
	return w, nil
}
func (a *fakeAdapter) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.workers) }

type fakeWorker struct {
	host    driverproto.WorkerHost
	open    func(context.Context, driverproto.OpenRequest) driverproto.OpenResult
	start   func(context.Context, driverproto.StartRequest) driverproto.StartResult
	control func(context.Context, driverproto.ControlRequest) driverproto.ControlResult
	retired atomic.Int32
	reaped  chan struct{}
	once    sync.Once
}

func (w *fakeWorker) Open(c context.Context, r driverproto.OpenRequest) driverproto.OpenResult {
	if w.open != nil {
		return w.open(c, r)
	}
	return driverproto.Ready()
}
func (w *fakeWorker) Start(c context.Context, r driverproto.StartRequest) driverproto.StartResult {
	return w.start(c, r)
}
func (w *fakeWorker) Control(c context.Context, r driverproto.ControlRequest) driverproto.ControlResult {
	if w.control != nil {
		return w.control(c, r)
	}
	return driverproto.ControlAccept(driverproto.KeepWorker)
}
func (w *fakeWorker) Retire()                 { w.retired.Add(1) }
func (w *fakeWorker) Reaped() <-chan struct{} { return w.reaped }
func (w *fakeWorker) reap()                   { w.once.Do(func() { close(w.reaped) }) }

type recorded struct {
	kind    string
	op      base.OpID
	turn    base.TurnID
	verdict base.ControlVerdict
}
type eventRecorder struct{ ch chan recorded }

func (r *eventRecorder) TurnStarted(op base.OpID, id base.TurnID) {
	r.ch <- recorded{kind: "started", op: op, turn: id}
}
func (r *eventRecorder) TurnRejected(op base.OpID, _, _ string) {
	r.ch <- recorded{kind: "rejected", op: op}
}
func (r *eventRecorder) Tool(_ base.TurnID, v base.ToolEvent) {
	r.ch <- recorded{kind: "tool-" + v.Phase}
}
func (r *eventRecorder) TurnEnded(id base.TurnID, _ base.TurnStatus, _, _ string) {
	r.ch <- recorded{kind: "ended", turn: id}
}
func (r *eventRecorder) ControlDone(op base.OpID, _ base.TurnID, verdict base.ControlVerdict, _ string) {
	r.ch <- recorded{kind: "control", op: op, verdict: verdict}
}
func (r *eventRecorder) ReadyDone(op base.OpID, v base.ReadyResult) {
	kind := "ready-failed"
	if v.Ready {
		kind = "ready"
	}
	r.ch <- recorded{kind: kind, op: op}
}
func (r *eventRecorder) ProviderLost(id base.TurnID, _ base.LostCause, _ string) {
	r.ch <- recorded{kind: "lost", turn: id}
}
func (r *eventRecorder) ResumeSeedUpdated([]byte) {}
func nextEvent(t *testing.T, r *eventRecorder) recorded {
	t.Helper()
	select {
	case v := <-r.ch:
		return v
	case <-time.After(time.Second):
		t.Fatal("event timeout")
		return recorded{}
	}
}
func testPolicy() Policy {
	p := DefaultPolicy()
	p.CommandAdmission = 100 * time.Millisecond
	p.OpenCall = time.Second
	p.StartCall = time.Second
	p.Started = time.Second
	p.Watchdog = time.Hour
	p.Reap = time.Second
	return p
}

func TestLazyOpenAndSynchronousEventReordering(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, FinalText: "ok"})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, err := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if a.count() != 0 {
		t.Fatal("runtime opened eagerly")
	}
	if err = e.Start(base.StartCommand{Op: "s1", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "hi"}}}, Scope: base.NewEffectScope("m", "m")}); err != nil {
		t.Fatal(err)
	}
	if v := nextEvent(t, r); v.kind != "started" {
		t.Fatalf("first=%+v", v)
	}
	if v := nextEvent(t, r); v.kind != "ended" {
		t.Fatalf("second=%+v", v)
	}
}

func TestTerminateDoesNotCrossReapedBarrier(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		return &fakeWorker{host: h, reaped: make(chan struct{}), start: func(context.Context, driverproto.StartRequest) driverproto.StartResult {
			return driverproto.StartAccept(driverproto.KeepWorker)
		}}
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	if err := e.EnsureReady("ready"); err != nil {
		t.Fatal(err)
	}
	if nextEvent(t, r).kind != "ready" {
		t.Fatal("not ready")
	}
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(base.StartCommand{Op: "next", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "next"}}}, Scope: base.NewEffectScope("n", "n")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if a.count() != 1 {
		t.Fatalf("opened %d workers before reap", a.count())
	}
	a.mu.Lock()
	first := a.workers[0]
	a.mu.Unlock()
	first.reap()
	deadline := time.Now().Add(time.Second)
	for a.count() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if a.count() != 2 {
		t.Fatal("did not reopen after reap")
	}
}

func TestTerminateReturnsToAbsentWithoutDemand(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		return &fakeWorker{host: h, reaped: make(chan struct{}), start: func(context.Context, driverproto.StartRequest) driverproto.StartResult {
			return driverproto.StartAccept(driverproto.KeepWorker)
		}}
	}
	r := &eventRecorder{ch: make(chan recorded, 4)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	if err := e.EnsureReady("ready"); err != nil {
		t.Fatal(err)
	}
	_ = nextEvent(t, r)
	if err := e.Terminate(); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	first := a.workers[0]
	a.mu.Unlock()
	first.reap()
	time.Sleep(30 * time.Millisecond)
	if got := a.count(); got != 1 {
		t.Fatalf("terminate reopened %d workers without demand", got)
	}
}

func TestWorkerEndedDuringOpenSettlesDemandWithoutTransparentRetry(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{}), start: func(context.Context, driverproto.StartRequest) driverproto.StartResult {
			return driverproto.StartAccept(driverproto.KeepWorker)
		}}
		w.open = func(ctx context.Context, _ driverproto.OpenRequest) driverproto.OpenResult {
			h.Events().Publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: "open transport ended"})
			<-ctx.Done()
			w.reap()
			return driverproto.OpenUncertain(driverproto.FailureTransport, ctx.Err().Error())
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 4)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	if err := e.Start(base.StartCommand{Op: "start", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")}); err != nil {
		t.Fatal(err)
	}
	if got := nextEvent(t, r); got.kind != "rejected" || got.op != "start" {
		t.Fatalf("opening failure result=%+v", got)
	}
	time.Sleep(30 * time.Millisecond)
	if got := a.count(); got != 1 {
		t.Fatalf("opening failure transparently created %d workers", got)
	}
}

func TestCloseRetiresWithoutWaitingForReaped(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		return &fakeWorker{host: h, reaped: make(chan struct{}), start: func(context.Context, driverproto.StartRequest) driverproto.StartResult {
			return driverproto.StartAccept(driverproto.KeepWorker)
		}}
	}
	r := &eventRecorder{ch: make(chan recorded, 2)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	if err := e.EnsureReady("r"); err != nil {
		t.Fatal(err)
	}
	_ = nextEvent(t, r)
	start := time.Now()
	e.Close()
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("Close waited for Reaped")
	}
	a.mu.Lock()
	w := a.workers[0]
	a.mu.Unlock()
	if w.retired.Load() != 1 {
		t.Fatalf("retire calls=%d", w.retired.Load())
	}
	if w.host.Events().Publish(driverproto.Diagnostic{Code: "tail"}) {
		t.Fatal("closed runtime accepted tail event")
	}
	if err := e.EnsureReady("x"); err != base.ErrRuntimeClosed {
		t.Fatalf("post-close error=%v", err)
	}
}

func TestStartResumeInvalidRetriesOriginalInputOnce(t *testing.T) {
	a := &fakeAdapter{}
	var got string
	a.factory = func(n int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		if n == 0 {
			w.start = func(context.Context, driverproto.StartRequest) driverproto.StartResult {
				go w.reap()
				return driverproto.StartInvalidResume("bad seed")
			}
		} else {
			w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
				got = r.Messages[0].Text
				target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "new"}
				h.Events().Publish(driverproto.TurnStarted{Target: target})
				h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK})
				return driverproto.StartAccept(driverproto.KeepWorker)
			}
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake", Capabilities: driverproto.Capabilities{Resume: true}}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, []byte("seed"), r)
	defer e.Close()
	if err := e.Start(base.StartCommand{Op: "start", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "original"}}}, Scope: base.NewEffectScope("m", "m")}); err != nil {
		t.Fatal(err)
	}
	if nextEvent(t, r).kind != "started" {
		t.Fatal("retry did not start")
	}
	if got != "original" {
		t.Fatalf("retried input=%q", got)
	}
}

func TestAdmissionTimeoutPoisonsAndEmergencyRetires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &fakeWorker{reaped: make(chan struct{}), start: func(context.Context, driverproto.StartRequest) driverproto.StartResult {
		return driverproto.StartAccept(driverproto.KeepWorker)
	}}
	sink := &eventSink{q: newQueue[eventEnvelope]()}
	e := &Engine{policy: Policy{CommandAdmission: 10 * time.Millisecond}.normalized(), root: ctx, cancel: cancel, commands: make(chan runtimeCommand), eventq: newQueue[eventEnvelope](), outbox: newQueue[func(base.RuntimeEvents)]()}
	e.fuse.handle = &emergencyHandle{worker: w, sink: sink, cancel: func() {}}
	err := e.EnsureReady("x")
	if err != base.ErrRuntimeUnavailable {
		t.Fatalf("error=%v", err)
	}
	if w.retired.Load() != 1 {
		t.Fatal("emergency retire not issued")
	}
}

type trackingBridge struct {
	old, new base.EffectScope
	mu       sync.Mutex
	invoked  []string
	acquired string
}

type settlingBridge struct {
	entered chan struct{}
	release chan struct{}
}

func (*settlingBridge) Catalog() []base.ToolSpec { return nil }
func (*settlingBridge) Acquire(base.EffectScope) (base.EffectLease, bool) {
	return base.NewEffectLease("ok"), true
}
func (b *settlingBridge) Invoke(context.Context, base.EffectLease, base.ToolInvocation) base.ToolResult {
	close(b.entered)
	<-b.release
	return base.ToolResult{Text: "ok"}
}

func (*trackingBridge) Catalog() []base.ToolSpec { return nil }
func (b *trackingBridge) Acquire(s base.EffectScope) (base.EffectLease, bool) {
	label := "unknown"
	if s == b.old {
		label = "old"
	}
	if s == b.new {
		label = "new"
	}
	b.mu.Lock()
	b.acquired = label
	b.mu.Unlock()
	return base.NewEffectLease(label), true
}
func (b *trackingBridge) Invoke(_ context.Context, l base.EffectLease, _ base.ToolInvocation) base.ToolResult {
	b.mu.Lock()
	b.invoked = append(b.invoked, b.acquired)
	b.mu.Unlock()
	return base.ToolResult{Text: "ok"}
}

func TestSteerScopeTransitionHoldsCallbackUntilControlDone(t *testing.T) {
	oldScope, newScope := base.NewEffectScope("old", "old"), base.NewEffectScope("new", "new")
	bridge := &trackingBridge{old: oldScope, new: newScope}
	var target driverproto.WorkerTurnTarget
	invokeDone := make(chan driverproto.ToolResult, 1)
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			target = driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		w.control = func(_ context.Context, _ driverproto.ControlRequest) driverproto.ControlResult {
			go func() {
				invokeDone <- h.Tools().Invoke(h.GenerationLife(), target, driverproto.ToolInvocation{CallID: "tool-1", Name: "call_actor"})
			}()
			time.Sleep(20 * time.Millisecond)
			bridge.mu.Lock()
			n := len(bridge.invoked)
			bridge.mu.Unlock()
			if n != 0 {
				t.Errorf("effect escaped pending transition")
			}
			return driverproto.ControlAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 16)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake", Capabilities: driverproto.Capabilities{Steer: true}}, testPolicy(), base.RuntimeDeps{Parent: context.Background(), Tools: bridge}, nil, r)
	defer e.Close()
	if err := e.Start(base.StartCommand{Op: "start", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "hi"}}}, Scope: oldScope}); err != nil {
		t.Fatal(err)
	}
	started := nextEvent(t, r)
	if started.kind != "started" {
		t.Fatalf("event=%+v", started)
	}
	if err := e.Control(base.ControlCommand{Op: "steer", Kind: base.RuntimeSteer, Target: started.turn, Content: &base.RuntimeInput{Text: "new"}, Scope: newScope}); err != nil {
		t.Fatal(err)
	}
	if v := nextEvent(t, r); v.kind != "control" {
		t.Fatalf("first after steer=%+v", v)
	}
	if v := nextEvent(t, r); v.kind != "tool-started" {
		t.Fatalf("second after steer=%+v", v)
	}
	select {
	case out := <-invokeDone:
		if out.IsError {
			t.Fatalf("tool error=%s", out.Text)
		}
	case <-time.After(time.Second):
		t.Fatal("held tool never released")
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if len(bridge.invoked) != 1 || bridge.invoked[0] != "new" {
		t.Fatalf("scope invocations=%v", bridge.invoked)
	}
}

func TestRejectedEffectRetryReusesDefinitiveError(t *testing.T) {
	results := make(chan driverproto.ToolResult, 2)
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, req driverproto.StartRequest) driverproto.StartResult {
			target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			go func() {
				invoke := driverproto.ToolInvocation{CallID: "same", Name: "call_actor"}
				results <- h.Tools().Invoke(req.Life, target, invoke)
				results <- h.Tools().Invoke(req.Life, target, invoke)
			}()
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 4)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	_ = nextEvent(t, r)
	for n := 0; n < 2; n++ {
		select {
		case got := <-results:
			if !got.IsError || got.Text != "tool bridge unavailable" {
				t.Fatalf("retry result=%+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("rejected effect retry blocked")
		}
	}
}

func TestTurnEndedWaitsForAdmittedEffect(t *testing.T) {
	bridge := &settlingBridge{entered: make(chan struct{}), release: make(chan struct{})}
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, req driverproto.StartRequest) driverproto.StartResult {
			target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			go h.Tools().Invoke(req.Life, target, driverproto.ToolInvocation{CallID: "tool", Name: "call_actor"})
			<-bridge.entered
			h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background(), Tools: bridge}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	if nextEvent(t, r).kind != "started" || nextEvent(t, r).kind != "tool-started" {
		t.Fatal("effect did not start")
	}
	select {
	case got := <-r.ch:
		t.Fatalf("terminal escaped settling: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
	close(bridge.release)
	if got := nextEvent(t, r); got.kind != "tool-ended" {
		t.Fatalf("first settled event=%+v", got)
	}
	if got := nextEvent(t, r); got.kind != "ended" {
		t.Fatalf("second settled event=%+v", got)
	}
}

func TestAcceptedActivityInvalidatesQueuedWatchdogTimer(t *testing.T) {
	target := driverproto.WorkerTurnTarget{Attempt: 1, Native: "native"}
	sink := &eventSink{generation: 1, q: newQueue[eventEnvelope]()}
	rev := sink.activate(target)
	turn := &runtimeTurn{serial: 1, target: target, phase: turnActive, canonical: "turn", watchRev: rev}
	g := &workerGeneration{id: 1, phase: workerReady, sink: sink, turn: turn}
	b := runtimeBook{worker: g}
	e := &Engine{policy: testPolicy()}
	if !sink.Publish(driverproto.Activity{Target: target}) {
		t.Fatal("activity not accepted")
	}
	e.onTimer(&b, timerDone{kind: timerWatchdog, gen: 1, serial: 1, rev: rev})
	if g.phase != workerReady {
		t.Fatal("accepted activity did not invalidate watchdog timer")
	}
}

func TestOversizedSteerRejectedBeforeWorkerControl(t *testing.T) {
	var controls atomic.Int32
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, req driverproto.StartRequest) driverproto.StartResult {
			h.Events().Publish(driverproto.TurnStarted{Target: driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: "native"}})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		w.control = func(context.Context, driverproto.ControlRequest) driverproto.ControlResult {
			controls.Add(1)
			return driverproto.ControlAccept(driverproto.KeepWorker)
		}
		return w
	}
	p := testPolicy()
	p.InputMaxBytes = 4
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake", Capabilities: driverproto.Capabilities{Steer: true}}, p, base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	started := nextEvent(t, r)
	_ = e.Control(base.ControlCommand{Op: "steer", Kind: base.RuntimeSteer, Target: started.turn, Content: &base.RuntimeInput{Text: "12345"}, Scope: base.NewEffectScope("n", "n")})
	got := nextEvent(t, r)
	if got.verdict != base.ControlInputTooLarge || controls.Load() != 0 {
		t.Fatalf("oversized steer result=%+v worker calls=%d", got, controls.Load())
	}
}

func TestNaturalTerminalWinsOverLateAmbiguousStartResult(t *testing.T) {
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK})
			time.Sleep(20 * time.Millisecond)
			return driverproto.StartUncertain(driverproto.FailureTransport, "late timeout")
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	if nextEvent(t, r).kind != "started" || nextEvent(t, r).kind != "ended" {
		t.Fatal("natural terminal missing")
	}
	select {
	case v := <-r.ch:
		if v.kind == "lost" {
			t.Fatal("late ambiguity overrode terminal")
		}
	case <-time.After(50 * time.Millisecond):
	}
	a.mu.Lock()
	w := a.workers[0]
	a.mu.Unlock()
	if w.retired.Load() == 0 {
		t.Fatal("ambiguous worker was reused")
	}
}

func TestOldAttemptTailCannotEndNewTurn(t *testing.T) {
	a := &fakeAdapter{}
	var old, newTarget driverproto.WorkerTurnTarget
	calls := 0
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			calls++
			target := driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: driverproto.WorkerTurnRef("native")}
			if calls == 1 {
				old = target
			} else {
				newTarget = target
			}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			if calls == 1 {
				h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK})
			}
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 12)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	start := func(op base.OpID) {
		if err := e.Start(base.StartCommand{Op: op, Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope(string(op), string(op))}); err != nil {
			t.Fatal(err)
		}
	}
	start("one")
	_ = nextEvent(t, r)
	_ = nextEvent(t, r)
	start("two")
	_ = nextEvent(t, r)
	a.mu.Lock()
	h := a.workers[0].host
	a.mu.Unlock()
	h.Events().Publish(driverproto.TurnEnded{Target: old, Status: driverproto.TurnOK})
	select {
	case v := <-r.ch:
		if v.kind == "ended" {
			t.Fatal("old attempt ended new turn")
		}
	case <-time.After(40 * time.Millisecond):
	}
	h.Events().Publish(driverproto.TurnEnded{Target: newTarget, Status: driverproto.TurnOK})
	if nextEvent(t, r).kind != "ended" {
		t.Fatal("new terminal missing")
	}
}

func TestTurnTerminalSettlesInFlightControlBeforeLateRPCResult(t *testing.T) {
	a := &fakeAdapter{}
	controlStarted := make(chan struct{})
	release := make(chan struct{})
	var target driverproto.WorkerTurnTarget
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			target = driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "native"}
			h.Events().Publish(driverproto.TurnStarted{Target: target})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		w.control = func(context.Context, driverproto.ControlRequest) driverproto.ControlResult {
			close(controlStarted)
			<-release
			return driverproto.ControlAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 12)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake", Capabilities: driverproto.Capabilities{Interrupt: true}}, testPolicy(), base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	started := nextEvent(t, r)
	_ = e.Control(base.ControlCommand{Op: "interrupt", Kind: base.RuntimeInterrupt, Target: started.turn})
	select {
	case <-controlStarted:
	case <-time.After(time.Second):
		t.Fatal("control not dispatched")
	}
	a.mu.Lock()
	h := a.workers[0].host
	a.mu.Unlock()
	h.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnInterrupted})
	if nextEvent(t, r).kind != "ended" {
		t.Fatal("terminal missing")
	}
	if v := nextEvent(t, r); v.kind != "control" || v.op != "interrupt" {
		t.Fatalf("control settlement=%+v", v)
	}
	close(release)
}

func TestWatchdogReportsLossBeforeReaped(t *testing.T) {
	p := testPolicy()
	p.Watchdog = 15 * time.Millisecond
	p.TerminalDrain = 10 * time.Millisecond
	a := &fakeAdapter{}
	a.factory = func(_ int, h driverproto.WorkerHost) *fakeWorker {
		w := &fakeWorker{host: h, reaped: make(chan struct{})}
		w.start = func(_ context.Context, r driverproto.StartRequest) driverproto.StartResult {
			h.Events().Publish(driverproto.TurnStarted{Target: driverproto.WorkerTurnTarget{Attempt: r.Attempt, Native: "native"}})
			return driverproto.StartAccept(driverproto.KeepWorker)
		}
		return w
	}
	r := &eventRecorder{ch: make(chan recorded, 8)}
	e, _ := New(a, driverproto.ProviderSpec{Name: "fake"}, p, base.RuntimeDeps{Parent: context.Background()}, nil, r)
	defer e.Close()
	_ = e.Start(base.StartCommand{Op: "s", Input: base.TurnInput{Messages: []base.RuntimeInput{{Text: "x"}}}, Scope: base.NewEffectScope("m", "m")})
	if nextEvent(t, r).kind != "started" {
		t.Fatal("start missing")
	}
	if nextEvent(t, r).kind != "lost" {
		t.Fatal("watchdog loss missing")
	}
	a.mu.Lock()
	w := a.workers[0]
	a.mu.Unlock()
	if w.retired.Load() == 0 {
		t.Fatal("watchdog did not hard-retire worker")
	}
}
