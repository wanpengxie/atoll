package coagent

import (
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestEmit_HappyPath_DefaultsToAgentText verifies the L2 §3.2.1
// shortest form `coagent emit "<text>"` writes a kind=event message
// of type agent.text with payload.text = the positional content.
func TestEmit_HappyPath_DefaultsToAgentText(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{"emit", "hello world"}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.Kind != string(v4types.KindEvent) {
		t.Fatalf("expected kind=event, got %q", out.Kind)
	}
	if out.ID == "" {
		t.Fatalf("expected non-empty id, got empty")
	}

	// Verify the envelope stored in mem reflects the positional text.
	env, err := f.store.FindByID(t.Context(), out.ID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if env == nil {
		t.Fatalf("envelope %q not stored", out.ID)
	}
	if env.Type != "agent.text" {
		t.Fatalf("expected type=agent.text, got %q", env.Type)
	}
	if !strings.Contains(string(env.Payload), `"text":"hello world"`) {
		t.Fatalf("payload missing text: %s", env.Payload)
	}
}

// TestEmit_TriggerCorrelationPropagates exercises the L1 §2.2.1
// first-tier fallback: with no --correlation-id flag, the envelope
// inherits trigger context.
func TestEmit_TriggerCorrelationPropagates(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{"emit", "ping"}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.CorrelationID != "trig-1" {
		t.Fatalf("expected correlation_id=trig-1 from trigger ctx, got %q", out.CorrelationID)
	}
}

// TestEmit_CorrelationIDNew triggers the second-tier fallback: CLI
// generates a UUID when --correlation-id new is passed. The fake id
// generator yields "id-1" for the envelope id and "id-2" for the
// fresh correlation id.
func TestEmit_CorrelationIDNew(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{"emit", "ping", "--correlation-id", "new"}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.CorrelationID == "trig-1" {
		t.Fatalf("expected fresh UUID, got trigger value %q", out.CorrelationID)
	}
	if out.CorrelationID != "id-2" {
		t.Fatalf("expected fakeIDGen second value 'id-2', got %q", out.CorrelationID)
	}
}

// TestEmit_CorrelationIDExplicit verifies a literal --correlation-id
// value bypasses fallback and writes envelope.correlation_id directly.
func TestEmit_CorrelationIDExplicit(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{"emit", "ping", "--correlation-id", "chain-x"}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.CorrelationID != "chain-x" {
		t.Fatalf("expected correlation_id=chain-x, got %q", out.CorrelationID)
	}
}

// TestEmit_PrivateShorthand expands --private to audience=<self> +
// visibility=private (L2 §3.3).
func TestEmit_PrivateShorthand(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{"emit", "note to self", "--private"}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	env, err := f.store.FindByID(t.Context(), out.ID)
	if err != nil || env == nil {
		t.Fatalf("envelope not stored: %v", err)
	}
	if env.Visibility != v4types.VisibilityPrivate {
		t.Fatalf("expected visibility=private, got %q", env.Visibility)
	}
	if len(env.Audience) != 1 || env.Audience[0] != "alice" {
		t.Fatalf("expected audience=[alice], got %v", env.Audience)
	}
}

// TestEmit_ExplicitPayloadOverridesPositional verifies --payload
// always wins over the positional "free text" form.
func TestEmit_ExplicitPayloadOverridesPositional(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{
		"emit",
		"--type", "agent.text",
		"--payload", `{"text":"explicit","extra":"k"}`,
		"this positional should be ignored",
	}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	env, _ := f.store.FindByID(t.Context(), out.ID)
	if env == nil {
		t.Fatalf("envelope %q not stored", out.ID)
	}
	if !strings.Contains(string(env.Payload), `"extra":"k"`) {
		t.Fatalf("expected explicit payload, got %s", env.Payload)
	}
	if strings.Contains(string(env.Payload), `"this positional`) {
		t.Fatalf("positional text should not be in payload: %s", env.Payload)
	}
}

// TestEmit_BadPayloadJSON surfaces parse error via stderr + exitFlagFormat.
func TestEmit_BadPayloadJSON(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{"emit", "--payload", "not-json"}, nil)
	if exit != exitFlagFormat {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitFlagFormat, exit, stderr)
	}
	if !strings.Contains(stderr, "--payload") {
		t.Fatalf("expected stderr to mention --payload, got %q", stderr)
	}
}
