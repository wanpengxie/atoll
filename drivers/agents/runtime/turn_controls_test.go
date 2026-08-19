package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type turnControlProvider struct {
	mu               sync.Mutex
	next             int
	opens            chan driverproto.OpenRequest
	starts           chan driverproto.StartRequest
	usage            driverproto.TurnUsage
	selections       []driverproto.TurnOptions
	defaultSelection int
}

func (p *turnControlProvider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: "turn-controls", Documentation: driverproto.Documentation{Description: "turn controls test"}, Selections: p.selections, DefaultSelection: p.defaultSelection}
}

func TestBuildCopiesProviderSelectionsIntoRuntimeSpec(t *testing.T) {
	provider := &turnControlProvider{selections: []driverproto.TurnOptions{{Model: "m", Effort: "low"}, {Model: "m", Effort: "high"}}, defaultSelection: 1}
	_, spec, err := Build(provider, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Selections) != 2 || spec.Selections[1] != (runtimeproto.TurnOptions{Model: "m", Effort: "high"}) || spec.DefaultSelection != 1 {
		t.Fatalf("spec=%+v", spec)
	}
}
func (p *turnControlProvider) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	p.mu.Lock()
	p.next++
	index := p.next
	p.mu.Unlock()
	return &turnControlWorker{provider: p, host: host, native: fmt.Sprintf("turn-%d", index), reaped: make(chan struct{})}, nil
}

type turnControlWorker struct {
	provider *turnControlProvider
	host     driverproto.WorkerHost
	native   string
	reaped   chan struct{}
	once     sync.Once
}

func (w *turnControlWorker) Open(_ context.Context, request driverproto.OpenRequest) {
	w.provider.opens <- request
	w.host.Events().Publish(driverproto.WorkerReady{})
}
func (w *turnControlWorker) Start(_ context.Context, request driverproto.StartRequest) {
	w.provider.starts <- request
	target := driverproto.WorkerTurnTarget{Attempt: request.Attempt, Native: driverproto.WorkerTurnRef(w.native)}
	w.host.Events().Publish(driverproto.TurnStarted{Target: target})
	usage := w.provider.usage
	if request.Kind == driverproto.TurnSelect {
		usage.Model, usage.Effort = request.Options.Model, request.Options.Effort
	}
	w.host.Events().Publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, Usage: usage})
}
func (*turnControlWorker) Control(context.Context, driverproto.ControlRequest) {}
func (w *turnControlWorker) Retire()                                           { w.once.Do(func() { close(w.reaped) }) }
func (w *turnControlWorker) Reaped() <-chan struct{}                           { return w.reaped }

func newTurnControlRuntime(t *testing.T, provider *turnControlProvider, options runtimeproto.TurnOptions) (runtimeproto.Runtime, *eventCollector) {
	t.Helper()
	provider.opens = make(chan driverproto.OpenRequest, 4)
	provider.starts = make(chan driverproto.StartRequest, 8)
	factory, _, err := Build(provider, Policy{OpenFactDeadline: time.Second, StartFactDeadline: time.Second, ReapedDemand: time.Second, Watchdog: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	events := newCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background()}, nil, options, events)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return rt, events
}

func TestSelectTurnUpdatesOptionsUsedByNextGenerationOpen(t *testing.T) {
	provider := &turnControlProvider{}
	initial := runtimeproto.TurnOptions{Model: "old", Effort: "low"}
	rt, events := newTurnControlRuntime(t, provider, initial)
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Kind: runtimeproto.TurnSelect, Options: runtimeproto.TurnOptions{Model: "new", Effort: "high"}}); err != nil {
		t.Fatal(err)
	}
	if got := <-provider.opens; got.Options.Model != "old" || got.Options.Effort != "low" {
		t.Fatalf("initial open=%+v", got.Options)
	}
	awaitKind(t, events, "ended")
	if err := rt.Terminate(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(runtimeproto.StartCommand{Op: 2, Messages: []runtimeproto.Input{{Text: "next"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-provider.opens:
		if got.Options.Model != "new" || got.Options.Effort != "high" {
			t.Fatalf("next open=%+v", got.Options)
		}
	case <-time.After(time.Second):
		t.Fatal("next generation did not open")
	}
}

func TestCompactAndSelectAllowEmptyMessages(t *testing.T) {
	provider := &turnControlProvider{}
	rt, events := newTurnControlRuntime(t, provider, runtimeproto.TurnOptions{})
	for op, kind := range []runtimeproto.TurnKind{runtimeproto.TurnCompact, runtimeproto.TurnSelect} {
		if err := rt.Start(runtimeproto.StartCommand{Op: runtimeproto.OpID(op + 1), Kind: kind, Options: runtimeproto.TurnOptions{Model: "m", Effort: "low"}}); err != nil {
			t.Fatal(err)
		}
		awaitKind(t, events, "ended")
	}
	if got := len(provider.starts); got != 2 {
		t.Fatalf("starts=%d want 2", got)
	}
}

func TestTurnEndedUsagePassesThroughRuntime(t *testing.T) {
	want := driverproto.TurnUsage{ContextTokens: 11, ContextWindow: 22, Model: "m", Effort: "high"}
	provider := &turnControlProvider{usage: want}
	rt, events := newTurnControlRuntime(t, provider, runtimeproto.TurnOptions{})
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Messages: []runtimeproto.Input{{Text: "hello"}}}); err != nil {
		t.Fatal(err)
	}
	got := awaitKind(t, events, "ended").usage
	if got.ContextTokens != want.ContextTokens || got.ContextWindow != want.ContextWindow || got.Model != want.Model || got.Effort != want.Effort {
		t.Fatalf("usage=%+v want=%+v", got, want)
	}
}
