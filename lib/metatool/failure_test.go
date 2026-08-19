package metatool

import (
	"strings"
	"testing"
)

func errObj(t *testing.T, rv ResultValue) map[string]any {
	t.Helper()
	obj, ok := rv.Value["error"].(map[string]any)
	if !ok {
		t.Fatalf("result carries no error object: %+v", rv.Value)
	}
	return obj
}

// Every one of these failures used to arrive as code "internal_error" with the
// hint "inspect error.detail and adapter logs before retrying", because the
// normaliser read the envelope's reason and every answering actor sets the same
// one. They call for four different responses, and an agent given the same
// sentence for all four has to guess which — the observed failure was one that
// guessed "transient" for a channel with no service agent installed.
func TestActorErrorCodeDecidesClassNotReason(t *testing.T) {
	const reason = "receiver_internal_error"
	cases := []struct {
		code      string
		want      ErrorCode
		retryable bool
	}{
		{"invalid_args", PayloadInvalid, false},
		{"permission_denied", PermissionDenied, false},
		{"no_service_agent", Unsupported, false},
		{"not_found", NotFound, false},
		{"conflict_exists", Conflict, false},
		{"type_unsupported", Unsupported, false},
		{"channel_unavailable", Unavailable, true},
		{"device_offline", ActorUnreachable, true},
	}
	for _, tc := range cases {
		payload := map[string]any{"error_code": tc.code, "detail": "the actor's own words"}
		obj := errObj(t, TerminalFailureToActorCLI("call_actor", "agent:x:1", "some.word", reason, payload))
		if got := obj["code"]; got != string(tc.want) {
			t.Errorf("%s classified as %v, want %v", tc.code, got, tc.want)
		}
		if got := obj["retryable"]; got != tc.retryable {
			t.Errorf("%s retryable=%v, want %v", tc.code, got, tc.retryable)
		}
		if msg, _ := obj["message"].(string); !strings.Contains(msg, tc.code) || !strings.Contains(msg, "the actor's own words") {
			t.Errorf("%s message hides the actor's verdict: %q", tc.code, msg)
		}
	}
}

// A state fact must not be described in words that invite another attempt, and
// a genuinely transient one must not be described as permanent.
func TestHintsDoNotInviteHopelessRetries(t *testing.T) {
	payload := map[string]any{"error_code": "no_service_agent", "detail": "channel has no service agent"}
	obj := errObj(t, TerminalFailureToActorCLI("call_actor", "peer:c1:1", "agent.ask", "receiver_internal_error", payload))
	hint, _ := obj["recovery_hint"].(string)
	if !strings.Contains(strings.ToLower(hint), "do not retry") {
		t.Errorf("no_service_agent hint does not say to stop: %q", hint)
	}

	payload = map[string]any{"error_code": "channel_unavailable", "detail": "momentarily gone"}
	obj = errObj(t, TerminalFailureToActorCLI("call_actor", "peer:c1:1", "agent.ask", "receiver_internal_error", payload))
	hint, _ = obj["recovery_hint"].(string)
	if !strings.Contains(strings.ToLower(hint), "retry may succeed") {
		t.Errorf("channel_unavailable hint does not offer the retry that would work: %q", hint)
	}
}

// An unrecognised code must not lose the old behaviour: the reason-based
// classification stays as the fallback so a new adapter's code still lands
// somewhere sane rather than being dropped.
func TestUnknownCodeFallsBackToReason(t *testing.T) {
	payload := map[string]any{"error_code": "some_new_adapter_code", "detail": "..."}
	obj := errObj(t, TerminalFailureToActorCLI("call_actor", "tool:x:1", "x.y", "unanswered_timeout", payload))
	if obj["code"] != string(Timeout) {
		t.Errorf("unknown code did not fall back to the reason: %v", obj["code"])
	}
}
