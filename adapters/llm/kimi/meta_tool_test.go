package kimi

import (
	"context"
	"encoding/json"
	"testing"

	gokimitypes "github.com/wanpengxie/go-kimi/pkg/kimi/types"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestListActorsIncludesDescriptions(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&ListActorsTool{bridge: b}).Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
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

func TestCallActorClosedSetPreflightErrors(t *testing.T) {
	b := metaToolBridge(t)
	tool := &CallActorTool{bridge: b}
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{
			name:   "unknown_actor",
			params: `{"actor_id":"tool:missing","type":"xhs.publish","payload":{}}`,
			want:   "unknown_actor",
		},
		{
			name:   "unknown_type",
			params: `{"actor_id":"tool:xhs","type":"xhs.missing","payload":{}}`,
			want:   "unknown_type",
		},
		{
			name:   "actor_type_mismatch",
			params: `{"actor_id":"tool:other","type":"xhs.publish","payload":{}}`,
			want:   "actor_type_mismatch",
		},
		{
			name:   "kind_disallowed",
			params: `{"actor_id":"tool:xhs","type":"xhs.note.archived","payload":{}}`,
			want:   "kind_disallowed",
		},
		{
			name:   "payload_invalid",
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
				b.dispatchToolResponse(metaResponseForRequest(req, tc.responsePayload))
			}()
			result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{"title":"hello"}}`))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			assertActorCLIErrorCode(t, result, tc.want)
		})
	}

	t.Run("timeout", func(t *testing.T) {
		b := metaToolBridge(t)
		b.cfg.ChannelContext.Types[0].MaxPendingMs = 1
		ipc := newMetaFakeIPC()
		ctx := context.WithValue(context.Background(), channelToolRuntimeKey{}, channelToolRuntime{
			ipc:     ipc,
			trigger: metaTrigger(),
		})
		result, err := (&CallActorTool{bridge: b}).Execute(ctx, json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish","payload":{}}`))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		assertActorCLIErrorCode(t, result, "timeout")
	})
}

func TestDescribeActorReturnsSkillDocAndTypes(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeActorTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{"actor_id":"tool:xhs"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	value := result.Value.Value.(map[string]any)
	if value["actor_id"] != "tool:xhs" || value["description"] != "XHS automation" || value["skill_doc"] == "" {
		t.Fatalf("actor value=%+v", value)
	}
	if value["ready"] != true || value["ready_reason"] != "ok" {
		t.Fatalf("actor readiness value=%+v", value)
	}
	types := value["types"].([]map[string]any)
	if len(types) != 2 {
		t.Fatalf("types len=%d want 2", len(types))
	}
}

func TestDescribeActorUnknownActorReturnsClosedSetError(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeActorTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{"actor_id":"tool:missing"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertActorCLIErrorCode(t, result, "unknown_actor")
}

func TestDescribeTypeReturnsFullConventionFields(t *testing.T) {
	b := metaToolBridge(t)
	result, err := (&DescribeTypeTool{bridge: b}).Execute(context.Background(), json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.publish"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	value := result.Value.Value.(map[string]any)
	if value["actor_id"] != "tool:xhs" || value["type"] != "xhs.publish" || value["description"] != "Publish a note" {
		t.Fatalf("describe_type value=%+v", value)
	}
	example := value["payload_example"].(map[string]any)
	if example["title"] != "hello" {
		t.Fatalf("payload_example=%+v", example)
	}
	fields := value["payload_fields"].([]adapter.FieldDoc)
	if len(fields) != 1 || fields[0].Name != "title" {
		t.Fatalf("payload_fields=%+v", fields)
	}
	errors := value["error_codes"].([]adapter.ErrorDoc)
	if len(errors) != 1 || errors[0].Code != "publish_timeout" {
		t.Fatalf("error_codes=%+v", errors)
	}
	if value["notes"] != "Requires logged-in browser." {
		t.Fatalf("notes=%v", value["notes"])
	}
}

func TestDescribeTypeClosedSetErrors(t *testing.T) {
	b := metaToolBridge(t)
	tool := &DescribeTypeTool{bridge: b}
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{
			name:   "unknown_type",
			params: `{"actor_id":"tool:xhs","type":"xhs.missing"}`,
			want:   "unknown_type",
		},
		{
			name:   "actor_type_mismatch",
			params: `{"actor_id":"tool:other","type":"xhs.publish"}`,
			want:   "actor_type_mismatch",
		},
		{
			name:   "kind_disallowed",
			params: `{"actor_id":"tool:xhs","type":"xhs.note.archived"}`,
			want:   "kind_disallowed",
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

func metaToolBridge(t *testing.T) *Bridge {
	t.Helper()
	cfg := Config{
		APIKey:         "fake-key",
		Model:          "fake-model",
		ProviderType:   "anthropic",
		WorkDir:        t.TempDir(),
		ChannelContext: metaChannelContext(),
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
				PayloadFields: []adapter.FieldDoc{{
					Name:        "title",
					Required:    true,
					Description: "Note title",
					Example:     "hello",
				}},
				ErrorCodes: []adapter.ErrorDoc{{
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
