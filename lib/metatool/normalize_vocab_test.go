package metatool

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/message"
)

func TestNormalizePayloadEmpty(t *testing.T) {
	result, err := NormalizePayload(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != "{}" {
		t.Fatalf("expected {}, got %q", string(result))
	}
}

func TestNormalizePayloadEmptyString(t *testing.T) {
	result, err := NormalizePayload(json.RawMessage(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != "{}" {
		t.Fatalf("expected {}, got %q", string(result))
	}
}

func TestNormalizePayloadNull(t *testing.T) {
	result, err := NormalizePayload(json.RawMessage("null"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != "{}" {
		t.Fatalf("expected {}, got %q", string(result))
	}
}

func TestNormalizePayloadValidJSON(t *testing.T) {
	input := json.RawMessage(`{"key":"value"}`)
	result, err := NormalizePayload(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != `{"key":"value"}` {
		t.Fatalf("expected %q, got %q", `{"key":"value"}`, string(result))
	}
	// Verify it is a clone (modifying result should not affect input).
	result[0] = 'X'
	if input[0] == 'X' {
		t.Fatal("expected NormalizePayload to clone the input")
	}
}

func TestNormalizePayloadInvalidJSON(t *testing.T) {
	_, err := NormalizePayload(json.RawMessage(`{broken`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestNormalizePayloadWhitespace(t *testing.T) {
	result, err := NormalizePayload(json.RawMessage("   "))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != "{}" {
		t.Fatalf("expected {}, got %q", string(result))
	}
}

func TestCloneRawJSONDoesNotAlias(t *testing.T) {
	original := json.RawMessage(`{"a":1}`)
	cloned := CloneRawJSON(original)
	if string(cloned) != string(original) {
		t.Fatalf("expected same content, got %q vs %q", string(cloned), string(original))
	}
	// Mutate the clone and verify the original is untouched.
	cloned[0] = 'X'
	if original[0] == 'X' {
		t.Fatal("CloneRawJSON must not alias the original slice")
	}
}

func TestCloneRawJSONEmpty(t *testing.T) {
	result := CloneRawJSON(nil)
	if result != nil {
		t.Fatalf("expected nil for empty input, got %q", string(result))
	}
	result = CloneRawJSON(json.RawMessage{})
	if result != nil {
		t.Fatalf("expected nil for zero-length input, got %q", string(result))
	}
}

func TestResponseFailureReasonTerminalFailure(t *testing.T) {
	raw := json.RawMessage(`{"terminal_failure_reason":"receiver_unavailable"}`)
	reason := ResponseFailureReason(raw)
	if reason != "receiver_unavailable" {
		t.Fatalf("expected receiver_unavailable, got %q", reason)
	}
}

func TestResponseFailureReasonStatusFailed(t *testing.T) {
	raw := json.RawMessage(`{"status":"failed","reason":"something_broke"}`)
	reason := ResponseFailureReason(raw)
	if reason != "something_broke" {
		t.Fatalf("expected something_broke, got %q", reason)
	}
}

func TestResponseFailureReasonStatusFailedNoReason(t *testing.T) {
	raw := json.RawMessage(`{"status":"failed"}`)
	reason := ResponseFailureReason(raw)
	if reason != "failed" {
		t.Fatalf("expected failed, got %q", reason)
	}
}

func TestResponseFailureReasonSuccess(t *testing.T) {
	raw := json.RawMessage(`{"status":"completed","data":"ok"}`)
	reason := ResponseFailureReason(raw)
	if reason != "" {
		t.Fatalf("expected empty reason for success, got %q", reason)
	}
}

func TestResponseFailureReasonInvalidJSON(t *testing.T) {
	raw := json.RawMessage(`not json`)
	reason := ResponseFailureReason(raw)
	if reason != "" {
		t.Fatalf("expected empty reason for invalid JSON, got %q", reason)
	}
}

func TestStringValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"string with spaces", "  hi  ", "hi"},
		{"int", 42, "42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StringValue(tt.in)
			if got != tt.want {
				t.Fatalf("StringValue(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestResultFromResponseSuccess(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"status": "completed", "data": "ok"})
	env := message.Envelope{
		ID:      "resp-1",
		Kind:    message.KindResponse,
		Payload: payload,
	}
	rv, isFailure := ResultFromResponse("call_actor", env)
	if isFailure {
		t.Fatal("expected isFailure=false for success")
	}
	if rv.IsError {
		t.Fatal("expected IsError=false for success")
	}
	if rv.Value["data"] != "ok" {
		t.Fatalf("expected data=ok, got %v", rv.Value["data"])
	}
}

func TestResultFromResponseFailure(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"status": "failed",
		"reason": "receiver_unavailable",
	})
	env := message.Envelope{
		ID:      "resp-fail",
		Kind:    message.KindResponse,
		Payload: payload,
	}
	rv, isFailure := ResultFromResponse("call_actor", env)
	if !isFailure {
		t.Fatal("expected isFailure=true for failed response")
	}
	if !rv.IsError {
		t.Fatal("expected IsError=true for failed response")
	}
	if rv.Value["error"] != "receiver_unavailable" {
		t.Fatalf("expected error=receiver_unavailable, got %v", rv.Value["error"])
	}
}

func TestResultFromResponseNonResponse(t *testing.T) {
	env := message.Envelope{
		ID:   "evt-1",
		Kind: message.KindEvent,
	}
	rv, isFailure := ResultFromResponse("call_actor", env)
	if !isFailure {
		t.Fatal("expected isFailure=true for non-response envelope")
	}
	if !rv.IsError {
		t.Fatal("expected IsError=true for non-response envelope")
	}
}
