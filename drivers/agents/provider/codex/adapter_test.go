package codex

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type inertSink struct{}

func (inertSink) Publish(driverproto.DriverEvent) bool { return true }

type inertToolPort struct{}

func (inertToolPort) Catalog() []driverproto.ToolSpec { return nil }
func (inertToolPort) Invoke(context.Context, driverproto.WorkerTurnTarget, driverproto.ToolInvocation) driverproto.ToolResult {
	return driverproto.ToolResult{}
}

type inertResourcePort struct{}

func (inertResourcePort) Invoke(context.Context, driverproto.WorkerTurnTarget, driverproto.ResourceInvocation) driverproto.ResourceResult {
	return driverproto.ResourceResult{}
}

type inertHost struct{ life context.Context }

func (h inertHost) GenerationLife() context.Context   { return h.life }
func (inertHost) Events() driverproto.EventSink       { return inertSink{} }
func (inertHost) Logger() *slog.Logger                { return slog.New(slog.DiscardHandler) }
func (inertHost) Tools() driverproto.ToolPort         { return inertToolPort{} }
func (inertHost) Resources() driverproto.ResourcePort { return inertResourcePort{} }

func TestNewWorkerIsPureAndResourceFree(t *testing.T) {
	var spawns atomic.Int32
	cfg := Config{Binary: "codex", WorkspaceDir: t.TempDir(), Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { spawns.Add(1); return nil, context.Canceled }}
	p := NewProvider(cfg)
	a, err := p.NewAdapter()
	if err != nil {
		t.Fatal(err)
	}
	w, err := a.NewWorker(inertHost{life: context.Background()})
	if err != nil {
		t.Fatal(err)
	}
	if spawns.Load() != 0 {
		t.Fatal("NewWorker spawned external resources")
	}
	w.Retire()
	<-w.Reaped()
}

func TestToolSummaryRedactsSecretsBeforePublishing(t *testing.T) {
	got := boundedToolSummary(itemWire{Type: "commandExecution", AggregatedOutput: "Authorization: top-secret"})
	if strings.Contains(got, "top-secret") || !strings.Contains(got, "[redacted]") {
		t.Fatalf("tool detail was not redacted: %q", got)
	}
}
