package metatool_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// ---------------------------------------------------------------------------
// test harness: a real Shell driven by a recording writer
// ---------------------------------------------------------------------------

// recWriter records every emitted envelope so tests can assert on the request
// the Shell built, and can optionally fail/reject the write.
type recWriter struct {
	mu      sync.Mutex
	written []message.Envelope
	err     error
	reject  harness.HarnessRejectReason
}

func (w *recWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return harness.WriteResult{}, w.err
	}
	if w.reject != "" {
		return harness.WriteResult{RejectReason: w.reject}, nil
	}
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recWriter) lastRequest(t *testing.T, timeout time.Duration) message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		w.mu.Lock()
		for i := len(w.written) - 1; i >= 0; i-- {
			if w.written[i].Kind == message.KindRequest {
				env := w.written[i]
				w.mu.Unlock()
				return env
			}
		}
		w.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no request envelope emitted")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// newExecShell builds a Shell over a recording writer. seq mints monotonic ids
// so the test can drive Deliver against the emitted request id.
func newExecShell(w *recWriter) *metatool.Shell {
	var seq int64
	return metatool.NewShell(metatool.ShellConfig{
		Pen:   w,
		Clock: func() time.Time { return time.UnixMilli(0) },
		EnvelopeID: func(_ int64) message.ID {
			seq++
			return message.ID("req-" + itoa(seq))
		},
	})
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func finalResp(parentID message.ID, payload map[string]any) *message.Envelope {
	body, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:       "resp-" + parentID,
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  body,
	}
}

// defaultRC returns a RuntimeContext marking a live turn.
func defaultRC() metatool.RuntimeContext {
	return metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope:      message.Envelope{ID: "trig-1", ChannelID: "ch-test"},
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
// ExecuteCallActor — validation
// ---------------------------------------------------------------------------

func TestExecuteCallActor_NilShell(t *testing.T) {
	rv := metatool.ExecuteCallActor(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_OutsideTurn(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_MissingActorID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_MissingType(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x"})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_InvalidJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteCallActor(context.Background(), json.RawMessage(`{bad`), sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_InvalidPayloadJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params := json.RawMessage(`{"actor_id":"tool:x","type":"x.do","payload":{invalid}}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_EmptyParams(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteCallActor(context.Background(), nil, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_WhitespaceActorID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "  ", "type": "x.do"})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

// ---------------------------------------------------------------------------
// ExecuteCallActor — dispatch (the emitted request + wait modes)
// ---------------------------------------------------------------------------

// TestExecuteCallActor_FanOutEmitsRequest pins the request the Shell builds:
// wait=false returns an immediate ack and the emitted envelope carries the
// type/audience/parent/correlation/payload the meta-tool supplied.
func TestExecuteCallActor_FanOutEmitsRequest(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:xhs",
		"type":     "xhs.publish",
		"payload":  map[string]any{"title": "hello"},
		"wait":     false,
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertNotError(t, rv)
	if rv.Value["status"] != "accepted" {
		t.Fatalf("wait=false want immediate ack, got %v", rv.Value)
	}

	req := w.lastRequest(t, time.Second)
	if req.Type != "xhs.publish" {
		t.Fatalf("emitted type = %q", req.Type)
	}
	if len(req.Audience) != 1 || req.Audience[0] != actor.ActorID("tool:xhs") {
		t.Fatalf("emitted audience = %v", req.Audience)
	}
	if req.ParentID != "trig-1" {
		t.Fatalf("emitted parent = %q", req.ParentID)
	}
	if req.CorrelationID != "corr-1" {
		t.Fatalf("emitted correlation = %q", req.CorrelationID)
	}
	if req.ExpiresAt == nil {
		t.Fatal("emitted request carries no ExpiresAt (author#2 deadline)")
	}
	var pl map[string]any
	_ = json.Unmarshal(req.Payload, &pl)
	if pl["title"] != "hello" {
		t.Fatalf("emitted payload = %v", pl)
	}
}

// TestExecuteCallActor_TimeoutResolverOverridesDefault pins P13: a
// ShellConfig.TimeoutResolver answer is the closure deadline actually used —
// it is NOT capped to DefaultTimeout (30s). A resolver answering 300s must
// surface as est_wait_ms=300000 on the immediate ack, not 30000.
func TestExecuteCallActor_TimeoutResolverOverridesDefault(t *testing.T) {
	w := &recWriter{}
	var seq int64
	sh := metatool.NewShell(metatool.ShellConfig{
		Pen:   w,
		Clock: func() time.Time { return time.UnixMilli(0) },
		EnvelopeID: func(_ int64) message.ID {
			seq++
			return message.ID("req-resolver-" + itoa(seq))
		},
		TimeoutResolver: func(target actor.ActorID, reqType string) (time.Duration, bool) {
			if target == "tool:slow" && reqType == "slow.op" {
				return 300 * time.Second, true
			}
			return 0, false
		},
	})

	params, _ := json.Marshal(map[string]any{
		"actor_id": "tool:slow", "type": "slow.op", "wait": false,
	})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertNotError(t, rv)
	if rv.Value["status"] != "accepted" {
		t.Fatalf("wait=false want immediate ack, got %v", rv.Value)
	}
	wantMs := int64(300 * time.Second / time.Millisecond)
	if got := rv.Value["est_wait_ms"]; got != wantMs {
		t.Fatalf("est_wait_ms = %v, want %d (resolver's 300s, not the 30s default)", got, wantMs)
	}

	req := w.lastRequest(t, time.Second)
	if req.ExpiresAt == nil {
		t.Fatal("emitted request carries no ExpiresAt")
	}
	if got := *req.ExpiresAt; got != int64(300*time.Second/time.Millisecond) {
		t.Fatalf("ExpiresAt = %d, want the resolver's 300s deadline", got)
	}
}

func TestExecuteCallActor_NilPayloadNormalizes(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do", "wait": false})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertNotError(t, rv)
	req := w.lastRequest(t, time.Second)
	if string(req.Payload) != "{}" {
		t.Fatalf("expected payload={}, got %s", string(req.Payload))
	}
}

// TestExecuteCallActor_SyncResolvesInline pins the sync experience: wait=true
// blocks until the final lands, then returns the response payload inline.
func TestExecuteCallActor_SyncResolvesInline(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)

	resCh := make(chan metatool.ResultValue, 1)
	go func() {
		params, _ := json.Marshal(map[string]any{
			"actor_id": "tool:xhs", "type": "xhs.publish", "wait": true,
		})
		resCh <- metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	}()

	req := w.lastRequest(t, 2*time.Second)
	sh.Deliver(finalResp(req.ID, map[string]any{"status": "completed", "note_id": "n1"}))

	select {
	case rv := <-resCh:
		assertNotError(t, rv)
		if rv.Value["note_id"] != "n1" {
			t.Fatalf("inline result = %v", rv.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sync call did not resolve after final")
	}
}

// TestExecuteCallActor_TerminalFailureNormalized pins the actor-CLI error
// mapping when the final response is a terminal failure.
func TestExecuteCallActor_TerminalFailureNormalized(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)

	resCh := make(chan metatool.ResultValue, 1)
	go func() {
		params, _ := json.Marshal(map[string]any{
			"actor_id": "tool:xhs", "type": "xhs.publish", "wait": true,
		})
		resCh <- metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	}()

	req := w.lastRequest(t, 2*time.Second)
	sh.Deliver(finalResp(req.ID, map[string]any{
		"status": "failed", "reason": string(message.TerminalReceiverUnavailable),
	}))

	select {
	case rv := <-resCh:
		assertIsError(t, rv, "actor_unreachable")
	case <-time.After(2 * time.Second):
		t.Fatal("sync call did not resolve after terminal failure")
	}
}

// TestShell_TimeoutArmsAuthor2Terminal pins the author#2 closure the Shell
// owns: a request nobody answers gets an unanswered_timeout terminal WRITTEN
// BY THE SHELL'S OWN CALLER TIMER (behavior.Caller) — the shell-level
// guarantee that "every request I send is guaranteed to close". Driven through
// ExecuteRequest with a tiny Timeout (the wall clock derives the deadline so a
// 50ms budget fires quickly).
func TestShell_TimeoutArmsAuthor2Terminal(t *testing.T) {
	w := &recWriter{}
	sh := metatool.NewShell(metatool.ShellConfig{
		Pen:        w,
		Clock:      time.Now, // real clock so ExpiresAt-now yields the budget
		EnvelopeID: func(_ int64) message.ID { return message.ID("req-dead") },
	})

	rv := sh.ExecuteRequest(context.Background(), defaultRC(), metatool.RequestSpec{
		ToolName: "call_actor", EnvelopeType: "dead.op", HandlerActorID: "tool:dead",
		WaitMode: metatool.WaitNone, Timeout: 50 * time.Millisecond,
	})
	if rv.Value["status"] != "accepted" {
		t.Fatalf("want ack, got %v", rv.Value)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		var term *message.Envelope
		for i := len(w.written) - 1; i >= 0; i-- {
			if w.written[i].Kind == message.KindResponse {
				e := w.written[i]
				term = &e
				break
			}
		}
		w.mu.Unlock()
		if term != nil {
			var p map[string]any
			_ = json.Unmarshal(term.Payload, &p)
			if p["status"] != "failed" || p["reason"] != string(message.TerminalUnansweredTimeout) {
				t.Fatalf("author#2 terminal payload: %+v", p)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("author#2 timeout terminal never written")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestShell_AbandonCallsCanceller pins M2: Abandon drops the local waiter AND
// (when the assembly root wires a Canceller) reaches the protocol-level
// cancel for the request's target — the ExecuteRequest/register plumbing that
// carries RequestSpec.HandlerActorID through to the pendingReq the Shell
// hands Abandon.
func TestShell_AbandonCallsCanceller(t *testing.T) {
	w := &recWriter{}
	var gotTarget actor.ActorID
	var gotReqID message.ID
	var calls int
	sh := metatool.NewShell(metatool.ShellConfig{
		Pen:        w,
		Clock:      func() time.Time { return time.UnixMilli(0) },
		EnvelopeID: func(_ int64) message.ID { return message.ID("req-abandon-1") },
		Canceller: func(target actor.ActorID, requestID message.ID) {
			calls++
			gotTarget = target
			gotReqID = requestID
		},
	})

	rv := sh.ExecuteRequest(context.Background(), defaultRC(), metatool.RequestSpec{
		ToolName: "call_actor", EnvelopeType: "cancel.probe", HandlerActorID: "tool:cancel-target",
		WaitMode: metatool.WaitNone, Timeout: time.Second,
	})
	if rv.Value["status"] != "accepted" {
		t.Fatalf("want ack, got %v", rv.Value)
	}

	sh.Abandon(message.ID("req-abandon-1"))

	if calls != 1 {
		t.Fatalf("canceller calls = %d, want 1", calls)
	}
	if gotTarget != actor.ActorID("tool:cancel-target") {
		t.Fatalf("canceller target = %q, want %q", gotTarget, "tool:cancel-target")
	}
	if gotReqID != message.ID("req-abandon-1") {
		t.Fatalf("canceller requestID = %q, want %q", gotReqID, "req-abandon-1")
	}
	if sh.InFlight(message.ID("req-abandon-1")) {
		t.Fatalf("Abandon must drop the local waiter (InFlight still true)")
	}

	// A second Abandon on the same (now-gone) id must not re-invoke the
	// canceller — Abandon only reaches a request it still holds locally.
	sh.Abandon(message.ID("req-abandon-1"))
	if calls != 1 {
		t.Fatalf("canceller calls after second Abandon = %d, want 1 (no-op on unknown id)", calls)
	}
}

// TestShell_AbandonWithoutCancellerIsLocalOnly pins nil-Canceller as the
// current-behavior default (ShellConfig.Canceller unset): Abandon still
// drops the local waiter, no protocol-level cancel is attempted.
func TestShell_AbandonWithoutCancellerIsLocalOnly(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := sh.ExecuteRequest(context.Background(), defaultRC(), metatool.RequestSpec{
		ToolName: "call_actor", EnvelopeType: "no.canceller", HandlerActorID: "tool:x",
		WaitMode: metatool.WaitNone, Timeout: time.Second,
	})
	reqID := message.ID(rv.Value["request_id"].(string))
	sh.Abandon(reqID) // must not panic with nil Canceller
	if sh.InFlight(reqID) {
		t.Fatalf("Abandon must drop the local waiter even with nil Canceller")
	}
}

func TestExecuteCallActor_WriteErrorIsInternal(t *testing.T) {
	w := &recWriter{reject: harness.HarnessRejectReason("harness_audience_invalid")}
	sh := newExecShell(w)
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do", "wait": false})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "internal_error")
	// The future must not leak after a rejected emit.
	if len(sh.Pending()) != 0 {
		t.Fatalf("future leaked after rejected emit: %v", sh.Pending())
	}
}

// ---------------------------------------------------------------------------
// ExecuteListActors
// ---------------------------------------------------------------------------

func TestExecuteListActors_NilShell(t *testing.T) {
	rv := metatool.ExecuteListActors(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_OutsideTurn(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteListActors(context.Background(), nil, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "")
}

func TestExecuteListActors_RequestTimesOut(t *testing.T) {
	// No Deliver — the reserved request never gets a catalog; list_actors
	// reports the still-pending/failed condition.
	w := &recWriter{}
	sh := newExecShell(w)
	// Drive with a context that cancels quickly so the unbounded reserved
	// wait returns without a final.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	rv := metatool.ExecuteListActors(ctx, nil, sh, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_Success(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)

	resCh := make(chan metatool.ResultValue, 1)
	go func() { resCh <- metatool.ExecuteListActors(context.Background(), nil, sh, defaultRC()) }()

	req := w.lastRequest(t, 2*time.Second)
	if req.Type != "actor.list" {
		t.Fatalf("expected actor.list, got %q", req.Type)
	}
	catalog := map[string]any{
		"status": "completed",
		"actors": []map[string]any{
			{"id": "tool:xhs", "kind": "tool", "present": true},
			{"id": "agent:research", "kind": "agent", "present": false},
		},
	}
	sh.Deliver(finalResp(req.ID, catalog))

	select {
	case rv := <-resCh:
		assertNotError(t, rv)
		actors, ok := rv.Value["actors"].([]map[string]any)
		if !ok {
			t.Fatalf("expected actors []map[string]any, got %T", rv.Value["actors"])
		}
		if len(actors) != 2 {
			t.Fatalf("expected 2 actors, got %d", len(actors))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("list_actors did not resolve after catalog")
	}
}

// ---------------------------------------------------------------------------
// ExecuteDescribeActor / ExecuteDescribeType — validation
// ---------------------------------------------------------------------------

func TestExecuteDescribeActor_NilShell(t *testing.T) {
	rv := metatool.ExecuteDescribeActor(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeActor_MissingActorID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteDescribeActor(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeActor_InvalidJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteDescribeActor(context.Background(), json.RawMessage(`{bad`), sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeActor_OutsideTurn(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
	rv := metatool.ExecuteDescribeActor(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeActor_EmitsActorDescribe(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	resCh := make(chan metatool.ResultValue, 1)
	go func() {
		params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
		resCh <- metatool.ExecuteDescribeActor(context.Background(), params, sh, defaultRC())
	}()
	req := w.lastRequest(t, 2*time.Second)
	if req.Type != "actor.describe" {
		t.Fatalf("expected actor.describe, got %q", req.Type)
	}
	if req.Audience[0] != actor.ActorID("tool:xhs") {
		t.Fatalf("audience = %v", req.Audience)
	}
	sh.Deliver(finalResp(req.ID, map[string]any{"status": "completed", "name": "xhs"}))
	select {
	case rv := <-resCh:
		assertNotError(t, rv)
	case <-time.After(2 * time.Second):
		t.Fatal("describe_actor did not resolve")
	}
}

func TestExecuteDescribeType_NilShell(t *testing.T) {
	rv := metatool.ExecuteDescribeType(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeType_MissingActorID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"type": "xhs.publish"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_MissingType(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_InvalidJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteDescribeType(context.Background(), json.RawMessage(`{bad`), sh, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteDescribeType_OutsideTurn(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish"})
	rv := metatool.ExecuteDescribeType(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeType_EmitsTypePayload(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	resCh := make(chan metatool.ResultValue, 1)
	go func() {
		params, _ := json.Marshal(map[string]any{"actor_id": "tool:xhs", "type": "xhs.publish"})
		resCh <- metatool.ExecuteDescribeType(context.Background(), params, sh, defaultRC())
	}()
	req := w.lastRequest(t, 2*time.Second)
	if req.Type != "actor.describe" {
		t.Fatalf("expected actor.describe, got %q", req.Type)
	}
	var pl map[string]string
	_ = json.Unmarshal(req.Payload, &pl)
	if pl["type"] != "xhs.publish" {
		t.Fatalf("emitted payload.type = %q", pl["type"])
	}
	sh.Deliver(finalResp(req.ID, map[string]any{"status": "completed", "type": "xhs.publish"}))
	select {
	case rv := <-resCh:
		assertNotError(t, rv)
	case <-time.After(2 * time.Second):
		t.Fatal("describe_type did not resolve")
	}
}

// ---------------------------------------------------------------------------
// ExecuteAwaitResult
// ---------------------------------------------------------------------------

func TestExecuteAwaitResult_NilShell(t *testing.T) {
	rv := metatool.ExecuteAwaitResult(context.Background(), nil, nil, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_MissingRequestID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_InvalidJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteAwaitResult(context.Background(), json.RawMessage(`{bad`), sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_NotInFlight(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"request_id": "req-unknown"})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_WhitespaceRequestID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"request_id": "  "})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

// submitInFlight fans out a wait=false call_actor so the Shell holds an
// in-flight future, and returns the emitted request id.
func submitInFlight(t *testing.T, w *recWriter, sh *metatool.Shell) message.ID {
	t.Helper()
	params, _ := json.Marshal(map[string]any{"actor_id": "tool:x", "type": "x.do", "wait": false})
	rv := metatool.ExecuteCallActor(context.Background(), params, sh, defaultRC())
	assertNotError(t, rv)
	return w.lastRequest(t, time.Second).ID
}

func TestExecuteAwaitResult_SuccessFinalDelivered(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	// A fan-out (wait=false) future does not buffer an early final — it only
	// resolves a parked Await. Deliver concurrently once await_result parks.
	go func() {
		time.Sleep(20 * time.Millisecond)
		sh.Deliver(finalResp(reqID, map[string]any{"status": "completed", "data": "hello"}))
	}()

	params, _ := json.Marshal(map[string]any{"request_id": reqID.String(), "timeout_ms": 5000})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	if rv.Name != "await_result" {
		t.Fatalf("expected Name=await_result, got %q", rv.Name)
	}
	assertNotError(t, rv)
	if rv.Value["data"] != "hello" {
		t.Fatalf("expected data=hello, got %v", rv.Value)
	}
}

func TestExecuteAwaitResult_Timeout(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	params, _ := json.Marshal(map[string]any{"request_id": reqID.String(), "timeout_ms": 50})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	if rv.Name != "await_result" {
		t.Fatalf("expected Name=await_result, got %q", rv.Name)
	}
	if status, _ := rv.Value["status"].(string); status != "accepted" {
		t.Fatalf("expected status=accepted, got %q; value=%v", status, rv.Value)
	}
}

func TestExecuteAwaitResult_ContextCancelled(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	params, _ := json.Marshal(map[string]any{"request_id": reqID.String()})
	rv := metatool.ExecuteAwaitResult(ctx, params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_CustomTimeout(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	go func() {
		time.Sleep(10 * time.Millisecond)
		sh.Deliver(finalResp(reqID, map[string]any{"status": "completed", "result": "done"}))
	}()
	params, _ := json.Marshal(map[string]any{"request_id": reqID.String(), "timeout_ms": 5000})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
}

func TestExecuteAwaitResult_FailedResponse(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	go func() {
		time.Sleep(20 * time.Millisecond)
		sh.Deliver(finalResp(reqID, map[string]any{"status": "failed", "reason": "adapter_error"}))
	}()
	params, _ := json.Marshal(map[string]any{"request_id": reqID.String(), "timeout_ms": 5000})
	rv := metatool.ExecuteAwaitResult(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "")
}

// ---------------------------------------------------------------------------
// ExecuteAbandon
// ---------------------------------------------------------------------------

func TestExecuteAbandon_NilShell(t *testing.T) {
	rv := metatool.ExecuteAbandon(context.Background(), nil, nil, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAbandon_MissingRequestID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{})
	rv := metatool.ExecuteAbandon(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_InvalidJSON(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteAbandon(context.Background(), json.RawMessage(`{bad`), sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_Success(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)

	params, _ := json.Marshal(map[string]any{"request_id": reqID.String()})
	rv := metatool.ExecuteAbandon(context.Background(), params, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
	if rv.Value["abandoned"] != reqID.String() {
		t.Fatalf("expected abandoned=%s, got %v", reqID, rv.Value["abandoned"])
	}
	if sh.InFlight(reqID) {
		t.Fatal("expected future cancelled after abandon")
	}
}

func TestExecuteAbandon_WhitespaceRequestID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"request_id": "  "})
	rv := metatool.ExecuteAbandon(context.Background(), params, sh, metatool.RuntimeContext{})
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAbandon_UnknownRequestID(t *testing.T) {
	sh := newExecShell(&recWriter{})
	params, _ := json.Marshal(map[string]any{"request_id": "req-nonexistent"})
	rv := metatool.ExecuteAbandon(context.Background(), params, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
}

// ---------------------------------------------------------------------------
// ExecuteListPending
// ---------------------------------------------------------------------------

func TestExecuteListPending_NilShell(t *testing.T) {
	rv := metatool.ExecuteListPending(context.Background(), nil, nil, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteListPending_EmptyList(t *testing.T) {
	sh := newExecShell(&recWriter{})
	rv := metatool.ExecuteListPending(context.Background(), nil, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
	if count, _ := rv.Value["count"].(int); count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}
	pending, ok := rv.Value["pending"].([]string)
	if !ok || len(pending) != 0 {
		t.Fatalf("expected empty []string pending, got %T %v", rv.Value["pending"], rv.Value["pending"])
	}
}

func TestExecuteListPending_WithPending(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	id1 := submitInFlight(t, w, sh)
	id2 := submitInFlight(t, w, sh)

	rv := metatool.ExecuteListPending(context.Background(), nil, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
	if count, _ := rv.Value["count"].(int); count != 2 {
		t.Fatalf("expected count=2, got %d", count)
	}
	pending, _ := rv.Value["pending"].([]string)
	found := map[string]bool{}
	for _, id := range pending {
		found[id] = true
	}
	if !found[id1.String()] || !found[id2.String()] {
		t.Fatalf("expected %s and %s in pending, got %v", id1, id2, pending)
	}
}

func TestExecuteListPending_AfterAbandon(t *testing.T) {
	w := &recWriter{}
	sh := newExecShell(w)
	reqID := submitInFlight(t, w, sh)
	sh.Abandon(reqID)

	rv := metatool.ExecuteListPending(context.Background(), nil, sh, metatool.RuntimeContext{})
	assertNotError(t, rv)
	if count, _ := rv.Value["count"].(int); count != 0 {
		t.Fatalf("expected count=0 after abandon, got %d", count)
	}
}
