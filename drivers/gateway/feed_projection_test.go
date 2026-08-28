package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestBuildBoundedFeedProjectsLargePayloadWithoutChangingLedgerEnvelope(t *testing.T) {
	originalPayload := json.RawMessage(`{"status":"processing","turn_id":"turn-1","process":{"kind":"tool","path":"/channel/image.png","output":{"image":"` + strings.Repeat("a", 700000) + `"}}}`)
	env := message.Envelope{
		ID: "progress-1", TS: 1, ChannelID: "c", Sender: message.Sender{Kind: actor.KindAgent, ID: "agent:a:1"},
		Kind: message.KindResponse, Type: "agent.ask", Payload: originalPayload,
		ParentID: "request-1", CorrelationID: "request-1", Visibility: message.VisibilityPublic,
		Audience: message.Audience{"human:root:1"},
	}
	result, err := buildBoundedFeed("", "c", 7, "live", 3, env)
	if err != nil {
		t.Fatal(err)
	}
	if !result.projected || len(result.encoded) > maxFeedFrameBytes {
		t.Fatalf("projected=%v frame_bytes=%d", result.projected, len(result.encoded))
	}
	if string(env.Payload) != string(originalPayload) {
		t.Fatal("transport projection mutated ledger envelope")
	}
	var projected message.Envelope
	if err := json.Unmarshal(result.envelope, &projected); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(projected.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "processing" || payload["turn_id"] != "turn-1" {
		t.Fatalf("small protocol facts lost: %s", projected.Payload)
	}
	process := payload["process"].(map[string]any)
	if process["path"] != "/channel/image.png" {
		t.Fatalf("small tool metadata lost: %s", projected.Payload)
	}
}

func TestBuildBoundedFeedLeavesSmallEnvelopeUntouched(t *testing.T) {
	env := message.Envelope{
		ID: "small", TS: 1, ChannelID: "c", Sender: message.Sender{Kind: actor.KindAgent, ID: "agent:a:1"},
		Kind: message.KindResponse, Type: "agent.ask", Payload: json.RawMessage(`{"status":"completed","text":"ok"}`),
		Visibility: message.VisibilityPublic, Audience: message.Audience{"human:root:1"},
	}
	want, _ := json.Marshal(env)
	result, err := buildBoundedFeed("history", "c", 8, "history", 4, env)
	if err != nil {
		t.Fatal(err)
	}
	if result.projected || string(result.envelope) != string(want) {
		t.Fatalf("projected=%v envelope=%s", result.projected, result.envelope)
	}
}
