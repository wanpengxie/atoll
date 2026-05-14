package adapter

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRespond records every Respond invocation under a mutex so tests
// can assert (requestID, opts.Status, opts.Reason) without spinning
// up a real harness.
type fakeRespond struct {
	mu    sync.Mutex
	calls []fakeRespondCall
	delay time.Duration
}

type fakeRespondCall struct {
	RequestID string
	Payload   json.RawMessage
	Opts      RespondOptions
}

func (f *fakeRespond) fn() RespondFn {
	return func(ctx context.Context, requestID string, payload json.RawMessage, opts RespondOptions) (RespondResult, error) {
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		f.mu.Lock()
		f.calls = append(f.calls, fakeRespondCall{
			RequestID: requestID,
			Payload:   append(json.RawMessage(nil), payload...),
			Opts:      opts,
		})
		f.mu.Unlock()
		return RespondResult{
			ID:            "response:" + requestID + ":fake",
			CorrelationID: requestID,
		}, nil
	}
}

func (f *fakeRespond) snapshot() []fakeRespondCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeRespondCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func TestErrorPolicy_TimeoutFiresAndCallsRespond(t *testing.T) {
	rec := &fakeRespond{}
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec.fn(), fixedClock(&clock), silentLogger())

	if err := policy.Timeout("req-1", 25, "adapter_default_timeout"); err != nil {
		t.Fatalf("Timeout: %v", err)
	}
	// Wait long enough for the timer to fire + the respond hook to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 fired Respond call, got %d", len(calls))
	}
	if calls[0].RequestID != "req-1" {
		t.Fatalf("RequestID = %q; want 'req-1'", calls[0].RequestID)
	}
	if calls[0].Opts.Status != StatusFailed {
		t.Fatalf("status = %q; want failed", calls[0].Opts.Status)
	}
	if calls[0].Opts.Reason != "adapter_default_timeout" {
		t.Fatalf("reason = %q; want adapter_default_timeout", calls[0].Opts.Reason)
	}
	// Timer map should be empty after fire.
	if got := policy.pendingTimerCount(); got != 0 {
		t.Fatalf("pendingTimerCount = %d; want 0", got)
	}
}

func TestErrorPolicy_CancelStopsTimer(t *testing.T) {
	rec := &fakeRespond{}
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec.fn(), fixedClock(&clock), silentLogger())

	if err := policy.Timeout("req-2", 100, "adapter_default_timeout"); err != nil {
		t.Fatalf("Timeout: %v", err)
	}
	policy.cancelTimer("req-2")
	// Wait past the original deadline; no respond should fire.
	time.Sleep(150 * time.Millisecond)
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("expected 0 respond calls after cancel, got %d", len(calls))
	}
	if policy.pendingTimerCount() != 0 {
		t.Fatalf("pendingTimerCount = %d; want 0", policy.pendingTimerCount())
	}
}

func TestErrorPolicy_ReregisterReplacesTimer(t *testing.T) {
	var fired int32
	rec := RespondFn(func(ctx context.Context, requestID string, payload json.RawMessage, opts RespondOptions) (RespondResult, error) {
		atomic.AddInt32(&fired, 1)
		return RespondResult{ID: "response:" + requestID + ":x"}, nil
	})
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec, fixedClock(&clock), silentLogger())

	if err := policy.Timeout("req-3", 10, "adapter_default_timeout"); err != nil {
		t.Fatalf("Timeout 1: %v", err)
	}
	if err := policy.Timeout("req-3", 200, "extended"); err != nil {
		t.Fatalf("Timeout 2: %v", err)
	}
	// First timer's 10ms must have been stopped; sleep > 10ms but < 200ms.
	time.Sleep(80 * time.Millisecond)
	if got := atomic.LoadInt32(&fired); got != 0 {
		t.Fatalf("first timer should have been replaced; fired=%d", got)
	}
	// Wait past the second deadline.
	time.Sleep(180 * time.Millisecond)
	if got := atomic.LoadInt32(&fired); got != 1 {
		t.Fatalf("second timer should have fired once; fired=%d", got)
	}
}

func TestErrorPolicy_FailTerminalCallsRespond(t *testing.T) {
	rec := &fakeRespond{}
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec.fn(), fixedClock(&clock), silentLogger())

	_, err := policy.FailTerminal(context.Background(), "req-4", "adapter_default_timeout", map[string]any{"http": 504})
	if err != nil {
		t.Fatalf("FailTerminal: %v", err)
	}
	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 Respond call, got %d", len(calls))
	}
	if calls[0].Opts.Detail["http"] != 504 {
		t.Fatalf("detail.http = %v; want 504", calls[0].Opts.Detail["http"])
	}
}

func TestErrorPolicy_ShutdownStopsTimers(t *testing.T) {
	rec := &fakeRespond{}
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec.fn(), fixedClock(&clock), silentLogger())

	_ = policy.Timeout("req-5", 50, "x")
	_ = policy.Timeout("req-6", 80, "y")
	if got := policy.pendingTimerCount(); got != 2 {
		t.Fatalf("pendingTimerCount = %d; want 2", got)
	}
	policy.shutdown()
	time.Sleep(120 * time.Millisecond)
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Fatalf("expected 0 respond calls after shutdown, got %d", len(calls))
	}
	if err := policy.Timeout("req-7", 10, "z"); err == nil || !strings.Contains(err.Error(), "shut down") {
		t.Fatalf("Timeout after shutdown should error 'shut down', got %v", err)
	}
}

func TestErrorPolicy_ValidationErrors(t *testing.T) {
	rec := &fakeRespond{}
	clock := int64(testT0)
	policy := newTimerPolicy("demo", rec.fn(), fixedClock(&clock), silentLogger())

	if err := policy.Timeout("", 10, "x"); err == nil || !strings.Contains(err.Error(), "requestID is required") {
		t.Fatalf("Timeout('') = %v; want requestID required", err)
	}
	if err := policy.Timeout("req", 0, "x"); err == nil || !strings.Contains(err.Error(), "afterMs must be > 0") {
		t.Fatalf("Timeout(0) = %v; want afterMs > 0", err)
	}
	if _, err := policy.FailTerminal(context.Background(), "", "x", nil); err == nil || !strings.Contains(err.Error(), "requestID is required") {
		t.Fatalf("FailTerminal('') = %v; want requestID required", err)
	}
}
