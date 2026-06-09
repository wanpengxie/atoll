package callkit_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/lib/callkit"
)

func TestNewError(t *testing.T) {
	rv := callkit.NewError("test_tool", callkit.PayloadInvalid, "bad payload", "fix it", map[string]string{"key": "val"})
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
	rv := callkit.NewError("t", callkit.InternalError, "oops", "", nil)
	errObj := rv.Value["error"].(map[string]any)
	if _, ok := errObj["recovery_hint"]; ok {
		t.Fatal("expected recovery_hint to be omitted when empty")
	}
	if _, ok := errObj["detail"]; ok {
		t.Fatal("expected detail to be omitted when nil")
	}
}

func TestPayloadInvalidError(t *testing.T) {
	rv := callkit.PayloadInvalidError("call_actor", "missing field", "check schema")
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
	original := callkit.ResultValue{
		Name:  "call_actor",
		Value: map[string]any{"data": "hello"},
	}
	result := callkit.NormalizeCallActorResult(original, "tool:xhs", "xhs.publish")
	if result.IsError {
		t.Fatal("expected IsError=false for non-error result")
	}
	if result.Value["data"] != "hello" {
		t.Fatalf("expected data=hello, got %v", result.Value["data"])
	}
}

func TestNormalizeCallActorResultNormalizesError(t *testing.T) {
	original := callkit.ResultValue{
		Name: "call_actor",
		Value: map[string]any{
			"error":   "receiver_unavailable",
			"payload": map[string]any{"detail": "gone"},
		},
	}
	result := callkit.NormalizeCallActorResult(original, "tool:xhs", "xhs.publish")
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
	original := callkit.ResultValue{
		Name:  "call_actor",
		Value: nil,
	}
	result := callkit.NormalizeCallActorResult(original, "a", "t")
	if result.Value != nil {
		t.Fatal("expected nil value to pass through")
	}
}

func TestNormalizeCallActorErrorNonError(t *testing.T) {
	isErr, val := callkit.NormalizeCallActorError("call_actor", false, map[string]any{"ok": true}, "a", "t")
	if isErr {
		t.Fatal("expected isErr=false for non-error")
	}
	m, _ := val.(map[string]any)
	if m["ok"] != true {
		t.Fatal("expected value to pass through unchanged")
	}
}

func TestNormalizeCallActorErrorWithReason(t *testing.T) {
	val := map[string]any{
		"error":   "unanswered_timeout",
		"payload": nil,
	}
	isErr, result := callkit.NormalizeCallActorError("call_actor", true, val, "tool:a", "a.do")
	if !isErr {
		t.Fatal("expected isErr=true")
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	errObj, _ := m["error"].(map[string]any)
	if errObj["code"] != "timeout" {
		t.Fatalf("expected code=timeout, got %v", errObj["code"])
	}
}

func TestNormalizeCallActorErrorGenericString(t *testing.T) {
	isErr, result := callkit.NormalizeCallActorError("call_actor", true, "something broke", "a", "t")
	if !isErr {
		t.Fatal("expected isErr=true")
	}
	m, _ := result.(map[string]any)
	errObj, _ := m["error"].(map[string]any)
	if errObj["code"] != "internal_error" {
		t.Fatalf("expected code=internal_error, got %v", errObj["code"])
	}
}

func TestNormalizeCallActorErrorTimedOut(t *testing.T) {
	isErr, result := callkit.NormalizeCallActorError("call_actor", true, "request timed out", "a", "t")
	if !isErr {
		t.Fatal("expected isErr=true")
	}
	m, _ := result.(map[string]any)
	errObj, _ := m["error"].(map[string]any)
	if errObj["code"] != "timeout" {
		t.Fatalf("expected code=timeout for 'timed out' message, got %v", errObj["code"])
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
			rv := callkit.TerminalFailureToActorCLI("tool", "actor:a", "a.do", tt.reason, nil)
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
