package script

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type testSink struct{ events chan driverproto.DriverEvent }

func (s *testSink) Publish(v driverproto.DriverEvent) bool { s.events <- v; return true }

type testTools struct{}

func (testTools) Catalog() []driverproto.ToolSpec { return nil }
func (testTools) Invoke(context.Context, driverproto.WorkerTurnTarget, driverproto.ToolInvocation) driverproto.ToolResult {
	return driverproto.ToolResult{Text: `{"text":"hello"}`}
}

type testResources struct {
	written chan driverproto.ResourceInvocation
}

func (r testResources) Invoke(_ context.Context, _ driverproto.WorkerTurnTarget, in driverproto.ResourceInvocation) driverproto.ResourceResult {
	r.written <- in
	return driverproto.ResourceResult{Payload: json.RawMessage(`{"ok":true}`)}
}

type testHost struct {
	life      context.Context
	sink      *testSink
	resources testResources
}

func (h testHost) GenerationLife() context.Context     { return h.life }
func (h testHost) Events() driverproto.EventSink       { return h.sink }
func (testHost) Logger() *slog.Logger                  { return slog.New(slog.DiscardHandler) }
func (testHost) Tools() driverproto.ToolPort           { return testTools{} }
func (h testHost) Resources() driverproto.ResourcePort { return h.resources }

func TestWorkerUsesPortsAndPublishesCompleteTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &testSink{events: make(chan driverproto.DriverEvent, 4)}
	writes := make(chan driverproto.ResourceInvocation, 1)
	w := newWorker("tool:echo", testHost{life: ctx, sink: sink, resources: testResources{written: writes}})
	if r := w.Open(ctx, driverproto.OpenRequest{}); r.Verdict() != driverproto.OpenReady {
		t.Fatalf("open=%v", r.Verdict())
	}
	life, stop := context.WithCancel(ctx)
	defer stop()
	res := w.Start(ctx, driverproto.StartRequest{Attempt: 7, Life: life, Messages: []driverproto.DriverMessage{{SourceID: "m1", Type: TypeChat, Payload: json.RawMessage(`{"text":"hello"}`), Text: "hello"}}})
	if res.Verdict() != driverproto.StartAccepted {
		t.Fatalf("start=%v", res.Verdict())
	}
	started := <-sink.events
	s, ok := started.(driverproto.TurnStarted)
	if !ok || !s.Target.Valid() || s.Target.Attempt != 7 {
		t.Fatalf("started=%#v", started)
	}
	select {
	case in := <-writes:
		if in.Operation != "write_file" || in.ResourceID != "file:loop/m1" {
			t.Fatalf("resource=%+v", in)
		}
	case <-time.After(time.Second):
		t.Fatal("resource call missing")
	}
	select {
	case event := <-sink.events:
		ended, ok := event.(driverproto.TurnEnded)
		if !ok || ended.Target != s.Target || ended.Status != driverproto.TurnOK {
			t.Fatalf("ended=%#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("turn end missing")
	}
	w.Retire()
	select {
	case <-w.Reaped():
	case <-time.After(time.Second):
		t.Fatal("worker did not reap")
	}
}
