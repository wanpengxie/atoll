package echo

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type recordingWriter struct {
	mu     sync.Mutex
	writes []*message.Envelope
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) last(t *testing.T) *message.Envelope {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		t.Fatal("no response written")
	}
	return w.writes[len(w.writes)-1]
}

func request(typ string, payload any) *message.Envelope {
	raw, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:        message.ID("req-" + typ),
		Kind:      message.KindRequest,
		Type:      typ,
		ChannelID: "ch-test",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Payload:   raw,
	}
}

func decodeResponse(t *testing.T, env *message.Envelope) (status string, raw map[string]json.RawMessage) {
	t.Helper()
	if env.Kind != message.KindResponse {
		t.Fatalf("response kind = %s; want response", env.Kind)
	}
	if err := json.Unmarshal(env.Payload, &raw); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	_ = json.Unmarshal(raw["status"], &status)
	return status, raw
}

func TestPingUsesBehaviorRespond(t *testing.T) {
	w := &recordingWriter{}
	a := NewActor(w)
	ctx := context.Background()

	_ = a.Receive(ctx, request(TypePing, map[string]any{"text": "ping"}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("status = %s", status)
	}
	if got := w.last(t).Type; got != TypePing {
		t.Fatalf("response type = %q; want %q", got, TypePing)
	}
	var echo bool
	_ = json.Unmarshal(raw["echo"], &echo)
	if !echo {
		t.Fatal("echo flag missing")
	}
	var originalID, originalType string
	_ = json.Unmarshal(raw["original_id"], &originalID)
	_ = json.Unmarshal(raw["original_type"], &originalType)
	if originalID != "req-"+TypePing || originalType != TypePing {
		t.Fatalf("original fields = %q/%q", originalID, originalType)
	}
}

func TestDescribeAndUnknownType(t *testing.T) {
	w := &recordingWriter{}
	a := NewActor(w)
	ctx := context.Background()

	_ = a.Receive(ctx, request("actor.describe", map[string]any{}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("describe status = %s", status)
	}
	var actorID string
	_ = json.Unmarshal(raw["actor_id"], &actorID)
	if actorID != string(DefaultActorID) {
		t.Fatalf("actor_id = %q; want %q", actorID, DefaultActorID)
	}
	var types map[string]json.RawMessage
	_ = json.Unmarshal(raw["types"], &types)
	if _, ok := types[TypePing]; !ok {
		t.Fatalf("describe missing type %s", TypePing)
	}

	_ = a.Receive(ctx, request("echo.nope", map[string]any{}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "failed" {
		t.Fatalf("unknown type status = %s; want failed", status)
	}
	var code string
	_ = json.Unmarshal(raw["error_code"], &code)
	if code != "type_unsupported" {
		t.Fatalf("unknown type error_code = %q; want type_unsupported", code)
	}
}
