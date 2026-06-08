package agent

import (
	"context"
	"encoding/json"
	"testing"

	gokimitypes "github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

func TestListActorsIncludesDescriptions(t *testing.T) {
	b := metaToolBridge(t)
	result := runMetaToolWithResponse(t, b, &ListActorsTool{bridge: b}, nil, metaActorListPayload(t))
	if result.IsError {
		t.Fatalf("list_actors returned error: %#v", result.Value.Value)
	}
	value := result.Value.Value.(map[string]any)
	actors := value["actors"].([]map[string]any)
	if len(actors) == 0 {
		t.Fatalf("actors empty")
	}
	var xhsActor map[string]any
	for _, candidate := range actors {
		if candidate["actor_id"] == "tool:xhs" {
			xhsActor = candidate
			break
		}
	}
	if xhsActor == nil {
		t.Fatalf("xhs actor missing from %+v", actors)
	}
	if xhsActor["description"] != "XHS automation" {
		t.Fatalf("actor description=%v", xhsActor["description"])
	}
	if xhsActor["ready"] != true || xhsActor["ready_reason"] != "ok" {
		t.Fatalf("actor readiness fields missing: %+v", xhsActor)
	}
	typesForActor := xhsActor["types"].([]map[string]any)
	if typesForActor[0]["description"] != "Archive notification" || typesForActor[1]["description"] != "Publish a note" {
		t.Fatalf("type descriptions not present: %+v", typesForActor)
	}
}

// TestCallActorLocalPreflightErrors covers the ONLY preflight checks that
// remain worker-side after the defrost: malformed params + missing required
// fields. Existence / kind / handler-binding validation is the daemon's job
// (the single source of truth) — call_actor no longer rejects an actor it
// thinks is unknown from a stale local copy, it emits and lets the daemon
// answer.
func TestCallActorLocalPreflightErrors(t *testing.T) {
	b := metaToolBridge(t)
	tool := &CallActorTool{bridge: b}
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{
			name:   "missing_actor_id",
			params: `{"type":"xhs.publish","payload":{}}`,
			want:   "payload_invalid",
		},
		{
			name:   "missing_type",
			params: `{"actor_id":"tool:xhs","payload":{}}`,
			want:   "payload_invalid",
		},
		{
			name:   "malformed_json",
			params: `{"actor_id":"tool:xhs","type":"xhs.publish","payload":`,
			want:   "payload_invalid",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := tool.Execute(context.Background(), json.RawMessage(tc.params))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			assertActorCLIErrorCode(t, result, tc.want)
		})
	}
}

// TestCallActorUnknownActorGoesToDaemon asserts that call_actor for an actor
// the worker has no local record of is NOT rejected with a stale-cache
// unknown_actor — it emits the envelope and surfaces the daemon's terminal.
// This is the tool:xhs-after-spawn regression guard: an actor that joined the
// channel after the worker spawned must be callable.
func TestCallActorUnknownActorGoesToDaemon(t *testing.T) {
	b := metaToolBridge(t)
	ipc := newMetaFakeIPC()
	ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
		ipc:     ipc,
		trigger: metaTrigger(),
	})
	emitted := make(chan message.Envelope, 1)
	go func() {
		req := <-ipc.writes
		emitted <- req
		resp := metaResponseForRequest(req, json.RawMessage(`{"status":"completed","ok":true}`)).Envelope
		b.caller().Deliver(&resp)
	}()
	// tool:late joined after spawn; the worker has no frozen snapshot of it.
	result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:late","type":"late.do","payload":{}}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	req := <-emitted
	if len(req.Audience) != 1 || req.Audience[0] != "tool:late" {
		t.Fatalf("request audience=%v want [tool:late] (must emit, not locally reject)", req.Audience)
	}
	if req.Type != "late.do" {
		t.Fatalf("request type=%q want late.do", req.Type)
	}
	if result.IsError {
		t.Fatalf("call_actor surfaced an error for a live round-trip: %#v", result.Value.Value)
	}
}

// TestCallActorLeavesExpiresAtUnset is the R2 regression guard: the worker
// MUST NOT stamp expires_at on an outbound call_actor request. The daemon
// harness (StepKindAndAudience) is the single authority on a type's
// max_pending_ms and stamps expires_at from the type registry ONLY when
// ExpiresAt==nil. If the worker pins expires_at=now+30s here, a long-running
// type that declares max_pending_ms > 30s (e.g. xhs long publish) would be
// falsely timed out by the worker's stamp instead of honoring its registered
// deadline. The caller-side fast-path ack WINDOW is a separate concern and is
// covered by the wait-mode tests above; this test only asserts the persisted
// closure deadline is left for the daemon.
func TestCallActorLeavesExpiresAtUnset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
	}{
		{name: "fast_path", params: `{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"hello"}}`},
		{name: "wait_true", params: `{"actor_id":"tool:xhs","type":"xhs.publish","payload":{},"wait":true}`},
		{name: "wait_false", params: `{"actor_id":"tool:xhs","type":"xhs.publish","payload":{},"wait":false}`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b := metaToolBridge(t)
			ipc := newMetaFakeIPC()
			ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
				ipc:     ipc,
				trigger: metaTrigger(),
			})
			emitted := make(chan message.Envelope, 1)
			go func() {
				req := <-ipc.writes
				emitted <- req
				resp := metaResponseForRequest(req, json.RawMessage(`{"status":"completed","ok":true}`)).Envelope
				b.caller().Deliver(&resp)
			}()
			if _, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(tc.params)); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			req := <-emitted
			if req.ExpiresAt != nil {
				t.Fatalf("worker stamped expires_at=%d; must leave nil so daemon stamps per-type max_pending_ms", *req.ExpiresAt)
			}
		})
	}
}

// TestExecuteReservedRequestLeavesExpiresAtUnset guards the reserved-type path
// (list_actors / describe_*) which shares the same daemon-owns-expires_at rule.
func TestExecuteReservedRequestLeavesExpiresAtUnset(t *testing.T) {
	b := metaToolBridge(t)
	ipc := newMetaFakeIPC()
	emitted := make(chan message.Envelope, 1)
	go func() {
		req := <-ipc.writes
		emitted <- req
		resp := metaResponseForRequest(req, metaActorListPayload(t)).Envelope
		b.caller().Deliver(&resp)
	}()
	go b.executeReservedRequestRaw(context.Background(), ipc, metaTrigger(), channelRequestSpec{
		ToolName:       "list_actors",
		EnvelopeType:   "actor.list",
		HandlerActorID: string(actor.SystemActorID),
		Payload:        json.RawMessage(`{}`),
		Timeout:        channelToolDefaultTimeout,
		WaitMode:       waitUnbounded,
	})
	req := <-emitted
	if req.ExpiresAt != nil {
		t.Fatalf("reserved request stamped expires_at=%d; must leave nil for daemon", *req.ExpiresAt)
	}
}

func TestCallActorClosedSetRuntimeErrors(t *testing.T) {
	cases := []struct {
		name            string
		responsePayload json.RawMessage
		want            string
	}{
		{
			name:            "actor_unreachable",
			responsePayload: json.RawMessage(`{"status":"failed","reason":"receiver_unavailable","error_code":"device_offline","detail":"offline"}`),
			want:            "actor_unreachable",
		},
		{
			name:            "internal_error",
			responsePayload: json.RawMessage(`{"status":"failed","reason":"receiver_internal_error","error_code":"tool_failed","detail":"boom"}`),
			want:            "internal_error",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b := metaToolBridge(t)
			ipc := newMetaFakeIPC()
			ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
				ipc:     ipc,
				trigger: metaTrigger(),
			})
			go func() {
				req := <-ipc.writes
				resp := metaResponseForRequest(req, tc.responsePayload).Envelope
				b.caller().Deliver(&resp)
			}()
			result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"hello"}}`))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			assertActorCLIErrorCode(t, result, tc.want)
		})
	}

	// Async fan-out (wait=false): the call returns an ACK immediately, not an
	// error — the request stays in flight (§2.3.2). A slow call is an async
	// hand-back, not a failure. Post-defrost the caller has no per-type
	// max_pending_ms to clamp the window with, so wait=false is the
	// deterministic ack path.
	t.Run("async_ack", func(t *testing.T) {
		b := metaToolBridge(t)
		ipc := newMetaFakeIPC()
		ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
			ipc:     ipc,
			trigger: metaTrigger(),
		})
		result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{},"wait":false}`))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.IsError {
			t.Fatalf("async fan-out should return an ack, not an error: %#v", result.Value.Value)
		}
		root, ok := result.Value.Value.(map[string]any)
		if !ok {
			t.Fatalf("ack value type=%T", result.Value.Value)
		}
		if root["status"] != "accepted" {
			t.Fatalf("ack status=%v want accepted", root["status"])
		}
		if root["request_id"] == "" || root["request_id"] == nil {
			t.Fatalf("ack missing request_id: %#v", root)
		}
		toWait, ok := root["to_wait"].(map[string]any)
		if !ok || toWait["tool"] != "await_result" {
			t.Fatalf("ack to_wait=%v", root["to_wait"])
		}
	})
}

// TestCallActorWaitTrueIsSync asserts wait=true awaits the final inline (sync
// opt-in) when the response arrives within the type timeout.
func TestCallActorWaitTrueIsSync(t *testing.T) {
	b := metaToolBridge(t)
	ipc := newMetaFakeIPC()
	ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
		ipc:     ipc,
		trigger: metaTrigger(),
	})
	go func() {
		req := <-ipc.writes
		resp := metaResponseForRequest(req, json.RawMessage(`{"status":"completed","note":"ok"}`)).Envelope
		b.caller().Deliver(&resp)
	}()
	result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{},"wait":true}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.IsError {
		t.Fatalf("wait=true sync call should return inline final: %#v", result.Value.Value)
	}
	value := result.Value.Value.(map[string]any)
	if value["note"] != "ok" {
		t.Fatalf("inline final value=%#v", value)
	}
}

// TestCallActorWaitFalseImmediateAck asserts wait=false returns an ack
// immediately without waiting for any response.
func TestCallActorWaitFalseImmediateAck(t *testing.T) {
	b := metaToolBridge(t)
	ipc := newMetaFakeIPC()
	ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
		ipc:     ipc,
		trigger: metaTrigger(),
	})
	result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{},"wait":false}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.IsError {
		t.Fatalf("wait=false should return an ack: %#v", result.Value.Value)
	}
	root := result.Value.Value.(map[string]any)
	if root["status"] != "accepted" {
		t.Fatalf("wait=false ack status=%v", root["status"])
	}
	reqID, _ := root["request_id"].(string)
	if reqID == "" {
		t.Fatal("wait=false ack missing request_id")
	}
	// The request must be in flight so a later await_result / trigger collects it.
	if len(b.caller().Pending()) != 1 {
		t.Fatalf("fan-out request not in flight: %v", b.caller().Pending())
	}
}

// TestDescribeActorReturnsLiveProjection asserts describe_actor surfaces the
// daemon's live actor.describe response verbatim (the framework intercept
// answers from the actor's current declaration; no frozen fallback).
func TestDescribeActorReturnsLiveProjection(t *testing.T) {
	b := metaToolBridge(t)
	resp := json.RawMessage(`{"status":"completed","actor_id":"tool:xhs","description":"XHS automation","skill_doc":"Use this actor to publish notes.","types":[{"type":"xhs.publish"},{"type":"xhs.note.archived"}]}`)
	result := runMetaToolWithResponse(t, b, &DescribeActorTool{bridge: b}, json.RawMessage(`{"actor_id":"tool:xhs"}`), resp)
	if result.IsError {
		t.Fatalf("describe_actor returned error: %#v", result.Value.Value)
	}
	value := result.Value.Value.(map[string]any)
	if value["actor_id"] != "tool:xhs" || value["description"] != "XHS automation" || value["skill_doc"] == "" {
		t.Fatalf("actor value=%+v", value)
	}
}

// TestDescribeActorMissingActorIsLocalPreflight asserts a missing actor_id is
// still caught locally (payload_invalid) without an envelope round-trip.
func TestDescribeActorMissingActorIsLocalPreflight(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeActorTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertActorCLIErrorCode(t, result, "payload_invalid")
}

// TestDescribeActorOutsideTurnIsInternalError asserts describe_actor with no
// live IPC returns a clean internal error rather than stale snapshot data.
func TestDescribeActorOutsideTurnIsInternalError(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeActorTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{"actor_id":"tool:xhs"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertActorCLIErrorCode(t, result, "internal_error")
}

// TestDescribeTypeReturnsLiveProjection asserts describe_type surfaces the
// daemon's live actor.describe(type-filtered) response.
func TestDescribeTypeReturnsLiveProjection(t *testing.T) {
	b := metaToolBridge(t)
	resp := json.RawMessage(`{"status":"completed","actor_id":"tool:xhs","type":"xhs.publish","description":"Publish a note","payload_example":{"title":"hello"},"notes":"Requires logged-in browser."}`)
	result := runMetaToolWithResponse(t, b, &DescribeTypeTool{bridge: b}, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish"}`), resp)
	if result.IsError {
		t.Fatalf("describe_type returned error: %#v", result.Value.Value)
	}
	value := result.Value.Value.(map[string]any)
	if value["actor_id"] != "tool:xhs" || value["type"] != "xhs.publish" || value["description"] != "Publish a note" {
		t.Fatalf("describe_type value=%+v", value)
	}
	example := value["payload_example"].(map[string]any)
	if example["title"] != "hello" {
		t.Fatalf("payload_example=%+v", example)
	}
	if value["notes"] != "Requires logged-in browser." {
		t.Fatalf("notes=%v", value["notes"])
	}
}

// TestDescribeTypeOutsideTurnIsInternalError mirrors describe_actor: no live
// IPC means a clean internal error, never stale local data.
func TestDescribeTypeOutsideTurnIsInternalError(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeTypeTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertActorCLIErrorCode(t, result, "internal_error")
}

func metaToolBridge(t *testing.T) *Bridge {
	t.Helper()
	cfg := Config{
		APIKey:       "fake-key",
		Model:        "fake-model",
		ProviderType: "anthropic",
		WorkDir:      t.TempDir(),
		NowFn: func() int64 {
			return 1_700_000_000_000
		},
	}
	b, err := NewBridge(cfg)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	return b
}

// runMetaToolWithResponse drives a meta tool whose Execute emits one envelope
// and awaits a response, feeding back respPayload as the (live) daemon reply.
// Models the actor.list / actor.describe round-trip now that no frozen
// snapshot exists worker-side.
func runMetaToolWithResponse(t *testing.T, b *Bridge, tool interface {
	Execute(context.Context, json.RawMessage) (gokimitypes.ToolResult, error)
}, params, respPayload json.RawMessage) gokimitypes.ToolResult {
	t.Helper()
	ipc := newMetaFakeIPC()
	ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
		ipc:     ipc,
		trigger: metaTrigger(),
	})
	go func() {
		req := <-ipc.writes
		resp := metaResponseForRequest(req, respPayload).Envelope
		b.caller().Deliver(&resp)
	}()
	result, err := tool.Execute(ctx, params)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return result
}

// metaActorListPayload is the actor.list response body the daemon would
// produce — the live channel catalog at call time, carrying the final
// status:completed the worker-side caller requires to treat it as final.
func metaActorListPayload(t *testing.T) json.RawMessage {
	t.Helper()
	cc := metaChannelContext()
	envelope := map[string]any{
		"status":       "completed",
		"channel_id":   cc.ChannelID,
		"channel_type": cc.ChannelType,
		"actors":       cc.Actors,
		"types":        cc.Types,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal actor.list payload: %v", err)
	}
	return raw
}

func metaChannelContext() ChannelContext {
	return ChannelContext{
		ChannelID: "ch-test",
		Actors: []ActorInfo{
			{
				ActorID:     "tool:xhs",
				Kind:        "tool",
				Binding:     "embedded",
				DisplayName: "xhs",
				Description: "XHS automation",
				SkillDoc:    "Use this actor to publish notes.",
				Ready:       true,
				ReadyReason: "ok",
				LastReadyAt: 1_700_000_000_000,
			},
			{
				ActorID:     "tool:other",
				Kind:        "tool",
				Binding:     "embedded",
				Description: "Other actor",
				Ready:       false,
				ReadyReason: "device_offline",
			},
		},
		Types: []TypeInfo{
			{
				Type:           "xhs.publish",
				HandlerActorID: "tool:xhs",
				HandlerBinding: "embedded",
				AllowedKinds:   []string{"request", "response"},
				MaxPendingMs:   50,
				Description:    "Publish a note",
				PayloadExample: json.RawMessage(`{"title":"hello"}`),
				PayloadFields: []FieldDoc{{
					Name:        "title",
					Required:    true,
					Description: "Note title",
					Example:     "hello",
				}},
				ErrorCodes: []ErrorDoc{{
					Code:     "publish_timeout",
					Recovery: "Retry after checking the browser",
				}},
				Notes: "Requires logged-in browser.",
			},
			{
				Type:           "xhs.note.archived",
				HandlerActorID: "tool:xhs",
				HandlerBinding: "embedded",
				AllowedKinds:   []string{"event"},
				Description:    "Archive notification",
			},
		},
	}
}

type metaFakeIPC struct {
	writes chan message.Envelope
}

func newMetaFakeIPC() *metaFakeIPC {
	return &metaFakeIPC{writes: make(chan message.Envelope, 1)}
}

func (f *metaFakeIPC) ChannelID() channel.ID           { return "ch-test" }
func (f *metaFakeIPC) WorkerID() string                { return "worker-test" }
func (f *metaFakeIPC) WorkerActorID() actor.ActorID    { return "agent:worker-test" }
func (f *metaFakeIPC) Triggers() <-chan TriggerPayload { return nil }
func (f *metaFakeIPC) WriteEnvelope(_ context.Context, env message.Envelope) error {
	f.writes <- env
	return nil
}

func metaTrigger() TriggerPayload {
	return TriggerPayload{
		Envelope: message.Envelope{
			ID:        "trigger-1",
			ChannelID: "ch-test",
			Type:      "human.text",
			Kind:      message.KindEvent,
			Sender:    message.Sender{Kind: actor.KindHuman, ID: "user:1"},
		},
		CorrelationID: "corr-1",
	}
}

func metaResponseForRequest(req message.Envelope, payload json.RawMessage) TriggerPayload {
	return TriggerPayload{
		Envelope: message.Envelope{
			ID:            message.ID("response-" + req.ID.String()),
			ChannelID:     req.ChannelID,
			Type:          req.Type,
			Kind:          message.KindResponse,
			Sender:        message.Sender{Kind: actor.KindTool, ID: req.Audience[0]},
			Audience:      message.Audience{req.Sender.ID},
			Payload:       payload,
			ParentID:      req.ID,
			CorrelationID: req.CorrelationID,
		},
		CorrelationID: req.CorrelationID,
	}
}

func assertActorCLIErrorCode(t *testing.T, result gokimitypes.ToolResult, want string) {
	t.Helper()
	if !result.IsError {
		t.Fatalf("IsError=false; value=%#v", result.Value.Value)
	}
	root, ok := result.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("value=%T want map", result.Value.Value)
	}
	errObj, ok := root["error"].(map[string]any)
	if !ok {
		t.Fatalf("error=%T want map in %#v", root["error"], root)
	}
	if got := errObj["code"]; got != want {
		t.Fatalf("error.code=%v want %s (value=%#v)", got, want, result.Value.Value)
	}
	if errObj["recovery_hint"] == "" {
		t.Fatalf("missing recovery_hint in %#v", errObj)
	}
}
