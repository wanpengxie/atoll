package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"
)

// recordingWriter captures every WriteResult attempt + caller_ctx for
// later inspection. Tests use it to assert envelope shape without
// spinning up a real harness.
type recordingWriter struct {
	mu       sync.Mutex
	calls    []writeCall
	overrideResult *pkgharness.WriteResult
	overrideErr    error
}

type writeCall struct {
	env    v4types.Envelope
	caller pkgharness.CallerCtx
}

func (r *recordingWriter) fn() WriterFn {
	return func(_ context.Context, env *v4types.Envelope, caller pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.calls = append(r.calls, writeCall{env: *env, caller: caller})
		if r.overrideErr != nil {
			return pkgharness.WriteResult{}, r.overrideErr
		}
		if r.overrideResult != nil {
			return *r.overrideResult, nil
		}
		return pkgharness.WriteResult{OK: true, Value: &pkgharness.Result{ID: env.ID, Kind: env.Kind}}, nil
	}
}

func (r *recordingWriter) snapshot() []writeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]writeCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func newTestBridge(t *testing.T) (*WireBridge, *recordingWriter) {
	t.Helper()
	w := &recordingWriter{}
	clk := int64(1700000000_000)
	b, err := NewWireBridge(BridgeConfig{
		ChannelID:            "ch-1",
		AgentID:              "alice",
		FencingToken:         3,
		TriggerCorrelationID: "corr-abc",
		Writer:               w.fn(),
		Clock:                func() int64 { clk += 1; return clk },
	})
	if err != nil {
		t.Fatalf("NewWireBridge: %v", err)
	}
	return b, w
}

// NewWireBridge enforces the required fields.
func TestNewWireBridge_Validates(t *testing.T) {
	cases := []struct {
		name string
		cfg  BridgeConfig
	}{
		{"missing channel_id", BridgeConfig{AgentID: "a", FencingToken: 1, Writer: func(context.Context, *v4types.Envelope, pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
			return pkgharness.WriteResult{}, nil
		}}},
		{"missing agent_id", BridgeConfig{ChannelID: "c", FencingToken: 1, Writer: func(context.Context, *v4types.Envelope, pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
			return pkgharness.WriteResult{}, nil
		}}},
		{"zero fencing_token", BridgeConfig{ChannelID: "c", AgentID: "a", Writer: func(context.Context, *v4types.Envelope, pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
			return pkgharness.WriteResult{}, nil
		}}},
		{"nil writer", BridgeConfig{ChannelID: "c", AgentID: "a", FencingToken: 1}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewWireBridge(tc.cfg); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

// TurnBegin → system-visibility agent.text with payload.kind=turn_begin.
func TestEmit_TurnBegin(t *testing.T) {
	b, w := newTestBridge(t)
	if err := b.Emit(wire.TurnBegin{TurnID: "turn-1"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	calls := w.snapshot()
	if len(calls) != 1 {
		t.Fatalf("calls=%d", len(calls))
	}
	c := calls[0]
	if c.env.Type != "agent.text" {
		t.Errorf("type=%q", c.env.Type)
	}
	if c.env.Kind != v4types.KindEvent {
		t.Errorf("kind=%q", c.env.Kind)
	}
	if c.env.Visibility != v4types.VisibilitySystem {
		t.Errorf("visibility=%q", c.env.Visibility)
	}
	if c.env.Sender.ID != "alice" || c.env.Sender.Kind != v4types.SenderAgent {
		t.Errorf("sender=%+v", c.env.Sender)
	}
	if c.env.CorrelationID != "corr-abc" {
		t.Errorf("correlation_id=%q", c.env.CorrelationID)
	}
	if c.caller.FencingToken != 3 || !c.caller.Authenticated {
		t.Errorf("caller_ctx=%+v", c.caller)
	}
	var payload map[string]any
	if err := json.Unmarshal(c.env.Payload, &payload); err != nil {
		t.Fatalf("payload parse: %v", err)
	}
	if payload["kind"] != "turn_begin" || payload["turn_id"] != "turn-1" {
		t.Errorf("payload=%+v", payload)
	}
}

// text_delta does NOT emit; turn_end consumes the buffer and produces a
// single public agent.text.
func TestEmit_TextDeltaBuffersUntilTurnEnd(t *testing.T) {
	b, w := newTestBridge(t)
	_ = b.Emit(wire.TurnBegin{TurnID: "t1"})
	if err := b.Emit(wire.TextDelta{TurnID: "t1", Delta: "hello "}); err != nil {
		t.Fatalf("delta1: %v", err)
	}
	if err := b.Emit(wire.TextDelta{TurnID: "t1", Delta: "world"}); err != nil {
		t.Fatalf("delta2: %v", err)
	}
	if got := len(w.snapshot()); got != 1 {
		// only turn_begin emitted so far
		t.Fatalf("pre-turn-end calls=%d", got)
	}

	if err := b.Emit(wire.TurnEnd{TurnID: "t1", StopReason: "stop"}); err != nil {
		t.Fatalf("turn_end: %v", err)
	}
	calls := w.snapshot()
	if len(calls) != 2 {
		t.Fatalf("calls=%d", len(calls))
	}
	final := calls[1]
	if final.env.Visibility != v4types.VisibilityPublic {
		t.Errorf("turn_end visibility=%q (want public)", final.env.Visibility)
	}
	var payload map[string]any
	if err := json.Unmarshal(final.env.Payload, &payload); err != nil {
		t.Fatalf("turn_end payload: %v", err)
	}
	if payload["text"] != "hello world" {
		t.Errorf("buffered text=%v", payload["text"])
	}
	if payload["stop_reason"] != "stop" {
		t.Errorf("stop_reason=%v", payload["stop_reason"])
	}
}

// TurnEnd.Output (ContentParts) overrides the buffered delta — when
// go-kimi finalizes the canonical output, the bridge prefers it.
func TestEmit_TurnEndPrefersOutputOverBuffer(t *testing.T) {
	b, w := newTestBridge(t)
	_ = b.Emit(wire.TurnBegin{TurnID: "t1"})
	_ = b.Emit(wire.TextDelta{TurnID: "t1", Delta: "partial"})

	out := types.ContentParts{types.TextPart{Text: "final"}}
	if err := b.Emit(wire.TurnEnd{TurnID: "t1", Output: out}); err != nil {
		t.Fatalf("turn_end: %v", err)
	}
	calls := w.snapshot()
	last := calls[len(calls)-1]
	var payload map[string]any
	_ = json.Unmarshal(last.env.Payload, &payload)
	if payload["text"] != "final" {
		t.Errorf("text=%v want 'final'", payload["text"])
	}
}

// All non-tool wire types route to system.event by default.
func TestEmit_SystemEventVariants(t *testing.T) {
	cases := []struct {
		name     string
		msg      wire.WireMessage
		wantKind string
	}{
		{"step_begin", wire.StepBegin{StepID: "s1", Name: "step"}, "step_begin"},
		{"step_interrupted", wire.StepInterrupted{StepID: "s1", Reason: "user"}, "step_interrupted"},
		{"steer_input", wire.SteerInput{Text: "stop"}, "steer_input"},
		{"status_update", wire.StatusUpdate{Status: "thinking"}, "status_update"},
		{"notification", wire.Notification{Message: "hi"}, "notification"},
		{"subagent_event", wire.SubagentEvent{AgentID: "sub1", EventType: "completed"}, "subagent_completed"},
		{"compaction_begin", wire.CompactionBegin{Trigger: "limit"}, "compaction_begin"},
		{"compaction_error", wire.CompactionError{Error: "oom"}, "compaction_error"},
		{"compaction_end", wire.CompactionEnd{Summary: "ok"}, "compaction_end"},
		{"mcp_loading_begin", wire.MCPLoadingBegin{}, "mcp_loading_begin"},
		{"mcp_loading_end", wire.MCPLoadingEnd{DurationMS: 12}, "mcp_loading_end"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b, w := newTestBridge(t)
			if err := b.Emit(tc.msg); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			calls := w.snapshot()
			if len(calls) != 1 {
				t.Fatalf("calls=%d", len(calls))
			}
			env := calls[0].env
			if env.Type != "system.event" {
				t.Errorf("type=%q", env.Type)
			}
			if env.Kind != v4types.KindEvent {
				t.Errorf("kind=%q", env.Kind)
			}
			if env.Visibility != v4types.VisibilitySystem {
				t.Errorf("visibility=%q", env.Visibility)
			}
			var payload map[string]any
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				t.Fatalf("parse payload: %v", err)
			}
			if payload["kind"] != tc.wantKind {
				t.Errorf("payload.kind=%v want %q", payload["kind"], tc.wantKind)
			}
		})
	}
}

// approval / question pairs produce public-visibility human.text
// envelopes with kind=request or kind=response.
func TestEmit_HumanTextVariants(t *testing.T) {
	cases := []struct {
		name         string
		msg          wire.WireMessage
		wantKind     v4types.Kind
		wantAudience []string
		wantPayKind  string
	}{
		{"approval_request", wire.ApprovalRequest{ID: "a1", Title: "Approve?"}, v4types.KindRequest, []string{"admin"}, "approval_request"},
		{"approval_response", wire.ApprovalResponse{RequestID: "a1", Approved: true}, v4types.KindResponse, []string{"admin"}, "approval_response"},
		{"question_request", wire.QuestionRequest{ID: "q1", Prompt: "what?"}, v4types.KindRequest, []string{"*"}, "question_request"},
		{"question_response", wire.QuestionResponse{RequestID: "q1"}, v4types.KindResponse, []string{"*"}, "question_response"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b, w := newTestBridge(t)
			if err := b.Emit(tc.msg); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			calls := w.snapshot()
			if len(calls) != 1 {
				t.Fatalf("calls=%d", len(calls))
			}
			env := calls[0].env
			if env.Type != "human.text" {
				t.Errorf("type=%q", env.Type)
			}
			if env.Kind != tc.wantKind {
				t.Errorf("kind=%q", env.Kind)
			}
			if env.Visibility != v4types.VisibilityPublic {
				t.Errorf("visibility=%q", env.Visibility)
			}
			if len(env.Audience) != len(tc.wantAudience) || env.Audience[0] != tc.wantAudience[0] {
				t.Errorf("audience=%v want %v", env.Audience, tc.wantAudience)
			}
			var payload map[string]any
			_ = json.Unmarshal(env.Payload, &payload)
			if payload["kind"] != tc.wantPayKind {
				t.Errorf("payload.kind=%v want %q", payload["kind"], tc.wantPayKind)
			}
		})
	}
}

// tool_call_request / tool_call_result are silently swallowed — the
// T11 v4 wrapper already emits these messages, so a duplicate write
// would trip Step 0.5 dedupe at best and confuse audit at worst.
func TestEmit_ToolCallSwallowed(t *testing.T) {
	b, w := newTestBridge(t)
	if err := b.Emit(wire.ToolCallRequest{ID: "tc1"}); err != nil {
		t.Fatalf("tool_call_request: %v", err)
	}
	if err := b.Emit(wire.ToolCallResult{ID: "tc1"}); err != nil {
		t.Fatalf("tool_call_result: %v", err)
	}
	if len(w.snapshot()) != 0 {
		t.Fatalf("tool_call must not emit, got %d calls", len(w.snapshot()))
	}
}

// question_option / question_item are sub-records of question_request
// and never standalone.
func TestEmit_QuestionSubrecordsSwallowed(t *testing.T) {
	b, w := newTestBridge(t)
	_ = b.Emit(wire.QuestionOption{Label: "yes", Value: "y"})
	_ = b.Emit(wire.QuestionItem{ID: "q1", Question: "ok?"})
	if got := len(w.snapshot()); got != 0 {
		t.Fatalf("question subrecords must not emit, got %d", got)
	}
}

// Same wire event → same envelope.id so harness step 0.5 can dedupe a
// replayed turn cleanly.
func TestEmit_DeterministicID(t *testing.T) {
	b1, w1 := newTestBridge(t)
	b2, w2 := newTestBridge(t)

	_ = b1.Emit(wire.TurnBegin{TurnID: "turn-X"})
	_ = b2.Emit(wire.TurnBegin{TurnID: "turn-X"})

	id1 := w1.snapshot()[0].env.ID
	id2 := w2.snapshot()[0].env.ID
	if id1 != id2 {
		t.Fatalf("deterministic id required: %q vs %q", id1, id2)
	}
}

// Reject from the writer is logged and swallowed — go-kimi's loop must
// continue even if a single observability write fails.
func TestEmit_RejectSwallowed(t *testing.T) {
	w := &recordingWriter{
		overrideResult: &pkgharness.WriteResult{
			OK:    false,
			Error: &pkgharness.RejectError{Reason: v4types.HarnessSenderMismatch, Detail: "test"},
		},
	}
	b, err := NewWireBridge(BridgeConfig{
		ChannelID: "ch", AgentID: "a", FencingToken: 1, Writer: w.fn(),
	})
	if err != nil {
		t.Fatalf("NewWireBridge: %v", err)
	}
	if err := b.Emit(wire.TurnBegin{TurnID: "t"}); err != nil {
		t.Fatalf("reject must not surface as error: %v", err)
	}
}

// Infrastructure errors (sql / driver) propagate so the caller can
// log + decide.
func TestEmit_InfraErrorPropagates(t *testing.T) {
	w := &recordingWriter{overrideErr: errors.New("boom")}
	b, err := NewWireBridge(BridgeConfig{
		ChannelID: "ch", AgentID: "a", FencingToken: 1, Writer: w.fn(),
	})
	if err != nil {
		t.Fatalf("NewWireBridge: %v", err)
	}
	if err := b.Emit(wire.TurnBegin{TurnID: "t"}); err == nil {
		t.Fatal("expected infra error to propagate")
	}
}

// Nil message → error, not panic.
func TestEmit_NilMessage(t *testing.T) {
	b, _ := newTestBridge(t)
	if err := b.Emit(nil); err == nil {
		t.Fatal("expected error on nil message")
	}
}

// Concurrent emits from multiple goroutines do not race on the buffer.
// Detected with `go test -race`.
func TestEmit_ConcurrentSafe(t *testing.T) {
	b, w := newTestBridge(t)
	_ = b.Emit(wire.TurnBegin{TurnID: "t1"})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Emit(wire.TextDelta{TurnID: "t1", Delta: "x"})
		}()
	}
	wg.Wait()
	_ = b.Emit(wire.TurnEnd{TurnID: "t1"})

	last := w.snapshot()[len(w.snapshot())-1]
	var payload map[string]any
	_ = json.Unmarshal(last.env.Payload, &payload)
	text, _ := payload["text"].(string)
	if len(text) != 50 {
		t.Fatalf("expected 50 chars buffered, got %d", len(text))
	}
}
