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

func (h *obligationLogHandler) record(i int) slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[i]
}

func recordAttrs(record slog.Record) map[string]slog.Value {
	attrs := map[string]slog.Value{}
	record.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	return attrs
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
	// The log line is the ONLY visibility surface for stranded obligations
	// (manual reclamation depends on it), so the contract is level+fields,
	// not just the message string.
	counts := handler.record(0)
	if counts.Level != slog.LevelWarn {
		t.Fatalf("counts level=%v want Warn", counts.Level)
	}
	attrs := recordAttrs(counts)
	if got := attrs["daemon"].String(); got != "daemon-a" {
		t.Fatalf("counts daemon=%q", got)
	}
	if got := attrs["channel"].String(); got != "channel-a" {
		t.Fatalf("counts channel=%q", got)
	}
	if r, v, tb := attrs["resources"].Int64(), attrs["reservations"].Int64(), attrs["tombstones"].Int64(); r != 1 || v != 2 || tb != 3 {
		t.Fatalf("counts resources=%d reservations=%d tombstones=%d", r, v, tb)
	}
	a.logOneDaemonObligation(context.Background(), "daemon-b", "channel-b", obligationCounterStub{err: errors.New("down")})
	if got := handler.messages(); len(got) != 2 || got[1] != "app.daemon.retired.counts_unknown" {
		t.Fatalf("failed counts logs=%v", got)
	}
	unknown := handler.record(1)
	if unknown.Level != slog.LevelWarn {
		t.Fatalf("counts_unknown level=%v want Warn", unknown.Level)
	}
	attrs = recordAttrs(unknown)
	if got := attrs["daemon"].String(); got != "daemon-b" {
		t.Fatalf("counts_unknown daemon=%q", got)
	}
	if got := attrs["channel"].String(); got != "channel-b" {
		t.Fatalf("counts_unknown channel=%q", got)
	}
	if _, ok := attrs["err"]; !ok {
		t.Fatalf("counts_unknown missing err attr")
	}
}
