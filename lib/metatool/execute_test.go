package metatool_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/callkit"
	"github.com/wanpengxie/ActOS/lib/metatool"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// ---------------------------------------------------------------------------
// mock types
// ---------------------------------------------------------------------------

// mockIPC implements metatool.IPC.
type mockIPC struct {
	channelID channel.ID
	actorID   actor.ActorID
	written   []message.Envelope
	writeErr  error
}

func (m *mockIPC) WriteEnvelope(_ context.Context, env message.Envelope) error {
	m.written = append(m.written, env)
	return m.writeErr
}

func (m *mockIPC) ChannelID() channel.ID        { return m.channelID }
func (m *mockIPC) WorkerActorID() actor.ActorID { return m.actorID }

// mockExecutor implements metatool.Executor.
type mockExecutor struct {
	// executeRequestFn is called by ExecuteRequest. If nil, returns a default success.
	executeRequestFn func(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue
	// executeReservedRawFn is called by ExecuteReservedRaw. If nil, returns (nil, false).
	executeReservedRawFn func(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) (json.RawMessage, bool)
	caller               *callkit.Client
}

func (m *mockExecutor) ExecuteRequest(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
	if m.executeRequestFn != nil {
		return m.executeRequestFn(ctx, rc, spec)
	}
	return metatool.ResultValue{Name: spec.ToolName, Value: map[string]any{"ok": true}}
}

func (m *mockExecutor) ExecuteReservedRaw(ctx context.Context, rc metatool.RuntimeContext, spec metatool.RequestSpec) (json.RawMessage, bool) {
	if m.executeReservedRawFn != nil {
		return m.executeReservedRawFn(ctx, rc, spec)
	}
	return nil, false
}

func (m *mockExecutor) CallerInstance() *callkit.Client {
	return m.caller
}

// newMockExec returns a mockExecutor with a real Caller.
func newMockExec() *mockExecutor {
	return &mockExecutor{caller: callkit.NewClient()}
}

// defaultRC returns a RuntimeContext with a working mockIPC.
func defaultRC() metatool.RuntimeContext {
	return metatool.RuntimeContext{
		IPC: &mockIPC{
			channelID: "ch-test",
			actorID:   "agent:test",
		},
		Trigger: metatool.Trigger{
			CorrelationID: "corr-1",
		},
	}
}

// assertIsError checks that rv.IsError is true and the error value contains code.
func assertIsError(t *testing.T, rv metatool.ResultValue, code string) {
	t.Helper()
	if !rv.IsError {
		t.Fatalf("expected IsError=true, got false; value=%v", rv.Value)
	}
	if code == "" {
		return
	}
	// Check nested error.code or top-level error string.
	errObj, ok := rv.Value["error"]
	if !ok {
		t.Fatalf("expected 'error' key in value; got %v", rv.Value)
	}
	if m, ok := errObj.(map[string]any); ok {
		if got, _ := m["code"].(string); got != code {
			t.Fatalf("expected error code %q, got %q; value=%v", code, got, rv.Value)
		}
	}
}

// assertNotError checks that rv.IsError is false.
func assertNotError(t *testing.T, rv metatool.ResultValue) {
	t.Helper()
	if rv.IsError {
		t.Fatalf("expected IsError=false, got true; value=%v", rv.Value)
	}
}

// ---------------------------------------------------------------------------
// ExecuteCallActor
// ---------------------------------------------------------------------------

func TestExecuteCallActor_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteCallActor(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_NilIPC(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do"})
	rc := metatool.RuntimeContext{IPC: nil}
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, rc)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_MissingActorID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_MissingType(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x"})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_InvalidJSON(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteCallActor(context.Background(), json.RawMessage(`{bad`), exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_InvalidPayloadJSON(t *testing.T) {
	exec := newMockExec()
	params := json.RawMessage(`{"actor_id":"tool:x","type":"x.do","payload":"not-json-object"}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	// "not-json-object" is valid JSON (a string), metatool.NormalizePayload accepts it.
	// Use truly invalid JSON in payload to trigger the error.
	_ = rv

	// Actually test with invalid JSON in the payload field itself. The payload
	// field is json.RawMessage, so we need to embed invalid JSON inside it.
	params2 := json.RawMessage(`{"actor_id":"tool:x","type":"x.do","payload":{invalid}}`)
	rv2 := metatool.ExecuteCallActor(context.Background(), params2, exec, defaultRC())
	// The outer unmarshal fails because {invalid} is not valid JSON.
	assertIsError(t, rv2, "payload_invalid")
}

func TestExecuteCallActor_SuccessDefaultWait(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{Name: "call_actor", Value: map[string]any{"ok": true}}
	}
	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:xhs",
		"type":     "xhs.publish",
		"payload":  map[string]any{"title": "hello"},
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	if captured.ToolName != "call_actor" {
		t.Fatalf("expected ToolName=call_actor, got %q", captured.ToolName)
	}
	if captured.EnvelopeType != "xhs.publish" {
		t.Fatalf("expected EnvelopeType=xhs.publish, got %q", captured.EnvelopeType)
	}
	if captured.HandlerActorID != "tool:xhs" {
		t.Fatalf("expected HandlerActorID=tool:xhs, got %q", captured.HandlerActorID)
	}
	if captured.WaitMode != metatool.WaitFastPath {
		t.Fatalf("expected metatool.WaitFastPath, got %d", captured.WaitMode)
	}
}

func TestExecuteCallActor_WaitTrue(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{Name: "call_actor", Value: map[string]any{"ok": true}}
	}
	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:xhs",
		"type":     "xhs.publish",
		"wait":     true,
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	if captured.WaitMode != metatool.WaitUnbounded {
		t.Fatalf("expected metatool.WaitUnbounded, got %d", captured.WaitMode)
	}
}

func TestExecuteCallActor_WaitFalse(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{Name: "call_actor", Value: map[string]any{"ok": true}}
	}
	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:xhs",
		"type":     "xhs.publish",
		"wait":     false,
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	if captured.WaitMode != metatool.WaitNone {
		t.Fatalf("expected metatool.WaitNone, got %d", captured.WaitMode)
	}
}

func TestExecuteCallActor_EmptyParams(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteCallActor(context.Background(), nil, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid") // actor_id required
}

func TestExecuteCallActor_WhitespaceActorID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "  ", "type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_NilPayloadNormalizes(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{Name: "call_actor", Value: map[string]any{"ok": true}}
	}
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	// Payload should be normalized to "{}" when omitted.
	if string(captured.Payload) != "{}" {
		t.Fatalf("expected payload={}, got %s", string(captured.Payload))
	}
}

// ---------------------------------------------------------------------------
// ExecuteListActors
// ---------------------------------------------------------------------------

func TestExecuteListActors_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteListActors(context.Background(), nil, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_NilIPC(t *testing.T) {
	exec := newMockExec()
	rc := metatool.RuntimeContext{IPC: nil}
	rv := metatool.ExecuteListActors(context.Background(), exec, rc)
	assertIsError(t, rv, "")
}

func TestExecuteListActors_RequestFails(t *testing.T) {
	exec := newMockExec()
	exec.executeReservedRawFn = func(_ context.Context, _ metatool.RuntimeContext, _ metatool.RequestSpec) (json.RawMessage, bool) {
		return nil, false
	}
	rv := metatool.ExecuteListActors(context.Background(), exec, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_DecodeError(t *testing.T) {
	exec := newMockExec()
	exec.executeReservedRawFn = func(_ context.Context, _ metatool.RuntimeContext, _ metatool.RequestSpec) (json.RawMessage, bool) {
		return json.RawMessage(`{not-valid-json`), true
	}
	rv := metatool.ExecuteListActors(context.Background(), exec, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_Success(t *testing.T) {
	exec := newMockExec()
	catalogJSON, _ := json.Marshal(map[string]any{
		"actors": []map[string]any{
			{"id": "tool:xhs", "kind": "tool", "present": true},
			{"id": "agent:research", "kind": "agent", "present": false},
		},
	})
	exec.executeReservedRawFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) (json.RawMessage, bool) {
		if spec.EnvelopeType != "actor.list" {
			t.Fatalf("expected EnvelopeType=actor.list, got %q", spec.EnvelopeType)
		}
		return catalogJSON, true
	}
	rv := metatool.ExecuteListActors(context.Background(), exec, defaultRC())
	assertNotError(t, rv)
	actors, ok := rv.Value["actors"].([]map[string]any)
	if !ok {
		t.Fatalf("expected actors to be []map[string]any, got %T", rv.Value["actors"])
	}
	if len(actors) != 2 {
		t.Fatalf("expected 2 actors, got %d", len(actors))
	}
}

func TestExecuteListActors_EmptyCatalog(t *testing.T) {
	exec := newMockExec()
	catalogJSON, _ := json.Marshal(map[string]any{"actors": []map[string]any{}})
	exec.executeReservedRawFn = func(_ context.Context, _ metatool.RuntimeContext, _ metatool.RequestSpec) (json.RawMessage, bool) {
		return catalogJSON, true
	}
	rv := metatool.ExecuteListActors(context.Background(), exec, defaultRC())
	assertNotError(t, rv)
	actors, ok := rv.Value["actors"].([]map[string]any)
	if !ok {
		t.Fatalf("expected actors key, got %v", rv.Value)
	}
	if len(actors) != 0 {
		t.Fatalf("expected empty actors, got %d", len(actors))
	}
}

// ---------------------------------------------------------------------------
// ExecuteDescribeActor
// ---------------------------------------------------------------------------

func TestExecuteDescribeActor_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteDescribeActor(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeActor_MissingActorID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteDescribeActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeActor_InvalidJSON(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteDescribeActor(context.Background(), json.RawMessage(`{bad`), exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeActor_NilIPC(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
	rc := metatool.RuntimeContext{IPC: nil}
	rv := metatool.ExecuteDescribeActor(context.Background(), params, exec, rc)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeActor_Success(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{
			Name:  "describe_actor",
			Value: map[string]any{"name": "xhs", "apis": []any{}},
		}
	}
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
	rv := metatool.ExecuteDescribeActor(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	if captured.ToolName != "describe_actor" {
		t.Fatalf("expected ToolName=describe_actor, got %q", captured.ToolName)
	}
	if captured.EnvelopeType != "actor.describe" {
		t.Fatalf("expected EnvelopeType=actor.describe, got %q", captured.EnvelopeType)
	}
	if captured.HandlerActorID != "tool:xhs" {
		t.Fatalf("expected HandlerActorID=tool:xhs, got %q", captured.HandlerActorID)
	}
}

func TestExecuteDescribeActor_WhitespaceActorID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "  "})
	rv := metatool.ExecuteDescribeActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeActor_EmptyParams(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteDescribeActor(context.Background(), nil, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

// ---------------------------------------------------------------------------
// ExecuteDescribeType
// ---------------------------------------------------------------------------

func TestExecuteDescribeType_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteDescribeType(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeType_MissingActorID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"type": "xhs.publish"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_MissingType(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_InvalidJSON(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteDescribeType(context.Background(), json.RawMessage(`{bad`), exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_NilIPC(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish"})
	rc := metatool.RuntimeContext{IPC: nil}
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, rc)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeType_Success(t *testing.T) {
	exec := newMockExec()
	var captured metatool.RequestSpec
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, spec metatool.RequestSpec) metatool.ResultValue {
		captured = spec
		return metatool.ResultValue{
			Name:  "describe_type",
			Value: map[string]any{"type": "xhs.publish", "payload_example": map[string]any{}},
		}
	}
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, defaultRC())
	assertNotError(t, rv)
	if captured.ToolName != "describe_type" {
		t.Fatalf("expected ToolName=describe_type, got %q", captured.ToolName)
	}
	if captured.EnvelopeType != "actor.describe" {
		t.Fatalf("expected EnvelopeType=actor.describe, got %q", captured.EnvelopeType)
	}
	if captured.HandlerActorID != "tool:xhs" {
		t.Fatalf("expected HandlerActorID=tool:xhs, got %q", captured.HandlerActorID)
	}
	// Verify the payload contains the type field.
	var payloadMap map[string]string
	if err := json.Unmarshal(captured.Payload, &payloadMap); err != nil {
		t.Fatalf("failed to unmarshal captured payload: %v", err)
	}
	if payloadMap["type"] != "xhs.publish" {
		t.Fatalf("expected payload.type=xhs.publish, got %q", payloadMap["type"])
	}
}

func TestExecuteDescribeType_BothEmpty(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_WhitespaceFields(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"actor_id": " ", "type": " "})
	rv := metatool.ExecuteDescribeType(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

// ---------------------------------------------------------------------------
// ExecuteAwaitResult
// ---------------------------------------------------------------------------

func TestExecuteAwaitResult_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteAwaitResult(context.Background(), nil, nil)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_MissingRequestID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_InvalidJSON(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteAwaitResult(context.Background(), json.RawMessage(`{bad`), exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_NotInFlight(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"request_id": "req-unknown"})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_SuccessFinalDelivered(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-42")

	// Register a future that expects await.
	exec.caller.Futures.Register(reqID, true)

	// Deliver a final response before Await is called (buffered for expectsAwait=true).
	finalEnv := &message.Envelope{
		ID:       "resp-42",
		ParentID: reqID,
		Kind:     message.KindResponse,
		Payload:  json.RawMessage(`{"status":"completed","data":"hello"}`),
	}
	exec.caller.Deliver(finalEnv)

	params, _ := json.Marshal(map[string]any{"request_id": "req-42"})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)

	// The result should come from metatool.ResultFromResponse on the final envelope.
	if rv.Name != "await_result" {
		t.Fatalf("expected Name=await_result, got %q", rv.Name)
	}
	// Since it has status=completed and data=hello, it should be a success.
	assertNotError(t, rv)
	if rv.Value["data"] != "hello" {
		t.Fatalf("expected data=hello, got %v", rv.Value)
	}
}

func TestExecuteAwaitResult_Timeout(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-timeout")

	// Register a future but don't deliver anything.
	exec.caller.Futures.Register(reqID, true)

	params, _ := json.Marshal(map[string]any{
		"request_id": "req-timeout",
		"timeout_ms": 50, // 50ms timeout
	})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)

	// Should get an ack-style result (still-pending), not an error.
	if rv.Name != "await_result" {
		t.Fatalf("expected Name=await_result, got %q", rv.Name)
	}
	// The result has status=accepted (ack).
	status, _ := rv.Value["status"].(string)
	if status != "accepted" {
		t.Fatalf("expected status=accepted, got %q; value=%v", status, rv.Value)
	}
}

func TestExecuteAwaitResult_ContextCancelled(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-cancel")

	exec.caller.Futures.Register(reqID, true)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately.
	cancel()

	params, _ := json.Marshal(map[string]any{"request_id": "req-cancel"})
	rv := metatool.ExecuteAwaitResult(ctx, params, exec)

	// Context cancellation should yield an error.
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_WhitespaceRequestID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"request_id": "  "})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_CustomTimeout(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-custom-to")
	exec.caller.Futures.Register(reqID, true)

	// Deliver the final quickly via a goroutine.
	go func() {
		time.Sleep(10 * time.Millisecond)
		finalEnv := &message.Envelope{
			ID:       "resp-custom",
			ParentID: reqID,
			Kind:     message.KindResponse,
			Payload:  json.RawMessage(`{"status":"completed","result":"done"}`),
		}
		exec.caller.Deliver(finalEnv)
	}()

	params, _ := json.Marshal(map[string]any{
		"request_id": "req-custom-to",
		"timeout_ms": 5000,
	})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)
	assertNotError(t, rv)
}

// ---------------------------------------------------------------------------
// ExecuteAbandon
// ---------------------------------------------------------------------------

func TestExecuteAbandon_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteAbandon(context.Background(), nil, nil)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAbandon_MissingRequestID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteAbandon(context.Background(), params, exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_InvalidJSON(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteAbandon(context.Background(), json.RawMessage(`{bad`), exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_Success(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-abn")

	// Register a future so there is something to abandon.
	exec.caller.Futures.Register(reqID, false)

	params, _ := json.Marshal(map[string]any{"request_id": "req-abn"})
	rv := metatool.ExecuteAbandon(context.Background(), params, exec)

	assertNotError(t, rv)
	if rv.Value["abandoned"] != "req-abn" {
		t.Fatalf("expected abandoned=req-abn, got %v", rv.Value["abandoned"])
	}
	if rv.Value["request_id"] != "req-abn" {
		t.Fatalf("expected request_id=req-abn, got %v", rv.Value["request_id"])
	}

	// Verify the future is gone.
	if exec.caller.Futures.Registered(reqID) {
		t.Fatal("expected future to be cancelled after abandon")
	}
}

func TestExecuteAbandon_WhitespaceRequestID(t *testing.T) {
	exec := newMockExec()
	params, _ := json.Marshal(map[string]any{"request_id": "  "})
	rv := metatool.ExecuteAbandon(context.Background(), params, exec)
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_UnknownRequestID(t *testing.T) {
	exec := newMockExec()
	// Abandon a non-existent request — should succeed (no-op).
	params, _ := json.Marshal(map[string]any{"request_id": "req-nonexistent"})
	rv := metatool.ExecuteAbandon(context.Background(), params, exec)
	assertNotError(t, rv)
}

// ---------------------------------------------------------------------------
// ExecuteListPending
// ---------------------------------------------------------------------------

func TestExecuteListPending_NilExecutor(t *testing.T) {
	rv := metatool.ExecuteListPending(context.Background(), nil)
	assertIsError(t, rv, "internal_error")
}

func TestExecuteListPending_EmptyList(t *testing.T) {
	exec := newMockExec()
	rv := metatool.ExecuteListPending(context.Background(), exec)
	assertNotError(t, rv)
	count, _ := rv.Value["count"].(int)
	if count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}
	pending, ok := rv.Value["pending"].([]string)
	if !ok {
		t.Fatalf("expected pending to be []string, got %T", rv.Value["pending"])
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty pending, got %d", len(pending))
	}
}

func TestExecuteListPending_WithPending(t *testing.T) {
	exec := newMockExec()
	exec.caller.Futures.Register("req-1", false)
	exec.caller.Futures.Register("req-2", true)

	rv := metatool.ExecuteListPending(context.Background(), exec)
	assertNotError(t, rv)
	count, _ := rv.Value["count"].(int)
	if count != 2 {
		t.Fatalf("expected count=2, got %d", count)
	}
	pending, ok := rv.Value["pending"].([]string)
	if !ok {
		t.Fatalf("expected pending to be []string, got %T", rv.Value["pending"])
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
	// Verify both request IDs are present (order may vary).
	found := map[string]bool{}
	for _, id := range pending {
		found[id] = true
	}
	if !found["req-1"] || !found["req-2"] {
		t.Fatalf("expected req-1 and req-2 in pending, got %v", pending)
	}
}

func TestExecuteListPending_AfterAbandon(t *testing.T) {
	exec := newMockExec()
	exec.caller.Futures.Register("req-x", false)

	// Abandon it.
	exec.caller.Abandon("req-x")

	rv := metatool.ExecuteListPending(context.Background(), exec)
	assertNotError(t, rv)
	count, _ := rv.Value["count"].(int)
	if count != 0 {
		t.Fatalf("expected count=0 after abandon, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// ExecuteCallActor — metatool.NormalizeCallActorResult integration
// ---------------------------------------------------------------------------

func TestExecuteCallActor_ErrorResultNormalized(t *testing.T) {
	exec := newMockExec()
	exec.executeRequestFn = func(_ context.Context, _ metatool.RuntimeContext, _ metatool.RequestSpec) metatool.ResultValue {
		return metatool.ResultValue{
			Name: "call_actor",
			Value: map[string]any{
				"error": "receiver_unavailable",
			},
			IsError: true,
		}
	}
	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:xhs",
		"type":     "xhs.publish",
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, exec, defaultRC())
	assertIsError(t, rv, "actor_unreachable")
}

// ---------------------------------------------------------------------------
// ExecuteAwaitResult — failed response envelope
// ---------------------------------------------------------------------------

func TestExecuteAwaitResult_FailedResponse(t *testing.T) {
	exec := newMockExec()
	reqID := message.ID("req-fail")
	exec.caller.Futures.Register(reqID, true)

	finalEnv := &message.Envelope{
		ID:       "resp-fail",
		ParentID: reqID,
		Kind:     message.KindResponse,
		Payload:  json.RawMessage(`{"status":"failed","reason":"adapter_error"}`),
	}
	exec.caller.Deliver(finalEnv)

	params, _ := json.Marshal(map[string]any{"request_id": "req-fail"})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, exec)
	assertIsError(t, rv, "")
}
