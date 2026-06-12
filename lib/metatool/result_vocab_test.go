package metatool

import (
	"testing"
)

func TestNewError(t *testing.T) {
	rv := NewError("test_tool", PayloadInvalid, "bad payload", "fix it", map[string]string{"key": "val"})
	if rv.Name != "test_tool" {
		t.Fatalf("expected name=test_tool, got %q", rv.Name)
	}
	if !rv.IsError {
		t.Fatal("expected IsError=true")
	}
	if rv.Value["ok"] != false {
		t.Fatalf("expected ok=false, got %v", rv.Value["ok"])
	}
	errObj, ok := rv.Value["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error to be map[string]any, got %T", rv.Value["error"])
	}
	if errObj["code"] != "payload_invalid" {
		t.Fatalf("expected code=payload_invalid, got %v", errObj["code"])
	}
	if errObj["message"] != "bad payload" {
		t.Fatalf("expected message=bad payload, got %v", errObj["message"])
	}
	if errObj["recovery_hint"] != "fix it" {
		t.Fatalf("expected recovery_hint=fix it, got %v", errObj["recovery_hint"])
	}
	if errObj["detail"] == nil {
		t.Fatal("expected detail to be present")
	}
}

func TestNewErrorOmitsEmptyHintAndNilDetail(t *testing.T) {
	rv := NewError("t", InternalError, "oops", "", nil)
	errObj := rv.Value["error"].(map[string]any)
	if _, ok := errObj["recovery_hint"]; ok {
		t.Fatal("expected recovery_hint to be omitted when empty")
	}
	if _, ok := errObj["detail"]; ok {
		t.Fatal("expected detail to be omitted when nil")
	}
}

func TestPayloadInvalidError(t *testing.T) {
	rv := PayloadInvalidError("call_actor", "missing field", "check schema")
	if rv.Name != "call_actor" {
		t.Fatalf("expected name=call_actor, got %q", rv.Name)
	}
	if !rv.IsError {
		t.Fatal("expected IsError=true")
	}
	errObj := rv.Value["error"].(map[string]any)
	if errObj["code"] != "payload_invalid" {
		t.Fatalf("expected code=payload_invalid, got %v", errObj["code"])
	}
}

func TestNormalizeCallActorResultPassesThrough(t *testing.T) {
	original := ResultValue{
		Name:  "call_actor",
		Value: map[string]any{"data": "hello"},
	}
	result := NormalizeCallActorResult(original, "tool:xhs", "xhs.publish")
	if result.IsError {
		t.Fatal("expected IsError=false for non-error result")
	}
	if result.Value["data"] != "hello" {
		t.Fatalf("expected data=hello, got %v", result.Value["data"])
	}
}

func TestNormalizeCallActorResultNormalizesError(t *testing.T) {
	original := ResultValue{
		Name: "call_actor",
		Value: map[string]any{
			"error":   "receiver_unavailable",
			"payload": map[string]any{"detail": "gone"},
		},
	}
	result := NormalizeCallActorResult(original, "tool:xhs", "xhs.publish")
	if !result.IsError {
		t.Fatal("expected IsError=true for error result")
	}
	errObj, ok := result.Value["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error to be map[string]any, got %T", result.Value["error"])
	}
	if errObj["code"] != "actor_unreachable" {
		t.Fatalf("expected code=actor_unreachable, got %v", errObj["code"])
	}
}

func TestNormalizeCallActorResultNilValue(t *testing.T) {
	original := ResultValue{
		Name:  "call_actor",
		Value: nil,
	}
	result := NormalizeCallActorResult(original, "a", "t")
	if result.Value != nil {
		t.Fatal("expected nil value to pass through")
	}
}

// A structured error (NewError: the CALL itself failed to build/emit/await)
// carries a MAP under "error", not a string reason — it is already clean and
// must pass through UNCHANGED. Regression guard: before, StringValue(map) →
// fmt.Sprint → the LLM saw "Actor returned failure 'map[code:… message:…]'"
// (double-wrapping a non-actor failure as an actor one).
func TestNormalizeCallActorResultPassesStructuredError(t *testing.T) {
	original := NewError("call_actor", InternalError,
		"emit channel request xhs.publish: link down", "retry", nil)
	result := NormalizeCallActorResult(original, "tool:xhs", "xhs.publish")
	if !result.IsError {
		t.Fatal("structured error must stay IsError=true")
	}
	errObj, ok := result.Value["error"].(map[string]any)
	if !ok {
		t.Fatalf("error must stay a structured map (not re-wrapped), got %T", result.Value["error"])
	}
	if errObj["code"] != string(InternalError) {
		t.Fatalf("code must be preserved, got %v", errObj["code"])
	}
	if msg, _ := errObj["message"].(string); msg != "emit channel request xhs.publish: link down" {
		t.Fatalf("message must pass through clean (no map[...] double-wrap), got %q", msg)
	}
}

func TestTerminalFailureToActorCLI(t *testing.T) {
	tests := []struct {
		reason   string
		wantCode string
	}{
		{"receiver_unavailable", "actor_unreachable"},
		{"unanswered_timeout", "timeout"},
		{"receiver_internal_error", "internal_error"},
		{"unknown_reason", "internal_error"},
	}
	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			rv := TerminalFailureToActorCLI("tool", "actor:a", "a.do", tt.reason, nil)
			if !rv.IsError {
				t.Fatal("expected IsError=true")
			}
			errObj := rv.Value["error"].(map[string]any)
			if errObj["code"] != tt.wantCode {
				t.Fatalf("expected code=%q, got %v", tt.wantCode, errObj["code"])
			}
		})
	}
}
