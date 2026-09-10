package metatool

import (
	"reflect"
	"testing"
)

func errObj(t *testing.T, rv ResultValue) map[string]any {
	t.Helper()
	obj, ok := rv.Value["error"].(map[string]any)
	if !ok {
		t.Fatalf("missing error: %+v", rv)
	}
	return obj
}
func TestActorOwnsFailureGuidance(t *testing.T) {
	for _, retry := range []bool{true, false} {
		payload := map[string]any{"error_code": "an_actor_specific_code", "detail": "actor explanation", "recovery_hint": "actor instructions", "retryable": retry}
		out := errObj(t, TerminalFailureToActorCLI("call_actor", "tool:a:1", "a.run", "receiver_internal_error", payload))
		if out["code"] != string(ActorError) || out["actor_code"] != payload["error_code"] || out["recovery_hint"] != payload["recovery_hint"] || out["retryable"] != retry || !reflect.DeepEqual(out["detail"], payload) {
			t.Fatalf("lost actor error: %+v", out)
		}
	}
}
func TestNoInferredBusinessRecoveryPolicy(t *testing.T) {
	for _, code := range []string{"capacity", "no_service_agent", "context_unavailable", "not_registered_anywhere"} {
		out := errObj(t, TerminalFailureToActorCLI("call_actor", "tool:a:1", "a.run", "receiver_internal_error", map[string]any{"error_code": code, "detail": "reason"}))
		if out["actor_code"] != code || out["code"] != string(ActorError) {
			t.Fatal(out)
		}
		if _, ok := out["retryable"]; ok {
			t.Fatal("invented retry policy", out)
		}
		if _, ok := out["recovery_hint"]; ok {
			t.Fatal("invented recovery guidance", out)
		}
	}
}
