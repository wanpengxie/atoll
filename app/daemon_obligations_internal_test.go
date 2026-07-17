package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
)

type obligationCounterStub struct {
	resources, reservations, tombstones int
	err                                 error
}

func (s obligationCounterStub) DaemonObligationCounts(context.Context, string) (int, int, int, error) {
	return s.resources, s.reservations, s.tombstones, s.err
}

type obligationLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *obligationLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *obligationLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock()
	return nil
}
func (h *obligationLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *obligationLogHandler) WithGroup(string) slog.Handler      { return h }
func (h *obligationLogHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.records))
	for i, record := range h.records {
		out[i] = record.Message
	}
	return out
}

func TestLogOneDaemonObligationEdgeContract(t *testing.T) {
	handler := &obligationLogHandler{}
	a := &App{logger: slog.New(handler)}
	a.logOneDaemonObligation(context.Background(), "daemon-a", "channel-a", obligationCounterStub{})
	if got := handler.messages(); len(got) != 0 {
		t.Fatalf("zero counts logged %v", got)
	}
	a.logOneDaemonObligation(context.Background(), "daemon-a", "channel-a", obligationCounterStub{resources: 1, reservations: 2, tombstones: 3})
	if got := handler.messages(); len(got) != 1 || got[0] != "app.daemon.retired.counts" {
		t.Fatalf("nonzero counts logs=%v", got)
	}
	a.logOneDaemonObligation(context.Background(), "daemon-b", "channel-b", obligationCounterStub{err: errors.New("down")})
	if got := handler.messages(); len(got) != 2 || got[1] != "app.daemon.retired.counts_unknown" {
		t.Fatalf("failed counts logs=%v", got)
	}
}
