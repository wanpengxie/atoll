package script

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type sink struct{ ch chan driverproto.DriverEvent }

func (s *sink) Publish(v driverproto.DriverEvent) bool { s.ch <- v; return true }

type host struct {
	life   context.Context
	events *sink
}

func (h host) GenerationLife() context.Context     { return h.life }
func (h host) Events() driverproto.EventSink       { return h.events }
func (h host) Tools() driverproto.ToolPort         { return nil }
func (h host) Resources() driverproto.ResourcePort { return nil }
func (h host) Logger() *slog.Logger                { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestWorkerCommandsReportOnlyThroughFactStream(t *testing.T) {
	events := &sink{ch: make(chan driverproto.DriverEvent, 8)}
	p := NewProvider("tool:test")
	w, err := p.NewWorker(host{life: context.Background(), events: events})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	w.Open(context.Background(), driverproto.OpenRequest{})
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("Open blocked on a semantic response")
	}
	if _, ok := (<-events.ch).(driverproto.WorkerReady); !ok {
		t.Fatal("Open did not publish WorkerReady")
	}
	w.Start(context.Background(), driverproto.StartRequest{Attempt: 1, Life: context.Background()})
	if got, ok := (<-events.ch).(driverproto.SubmissionRejected); !ok || got.Attempt != 1 {
		t.Fatalf("start fact=%#v", got)
	}
	w.Retire()
	select {
	case <-w.Reaped():
	case <-time.After(time.Second):
		t.Fatal("worker did not reap")
	}
}
