package coagent

import (
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestAsk_HappyPath_ExplicitAudience exercises the canonical
// agent → agent ask form: explicit --audience + core type
// `agent.text`. Harness checks audience exists + handler_actor_id
// is NULL so no mismatch.
func TestAsk_HappyPath_ExplicitAudience(t *testing.T) {
	f := newBusFixture(t)
	exit, stdout, stderr := f.runCLI([]string{
		"ask", "--type", "agent.text", "--audience", "bob",
		"how's the impl going?",
	}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.Kind != string(v4types.KindRequest) {
		t.Fatalf("expected kind=request, got %q", out.Kind)
	}
	env, _ := f.store.FindByID(t.Context(), out.ID)
	if env == nil {
		t.Fatalf("envelope not stored")
	}
	if len(env.Audience) != 1 || env.Audience[0] != "bob" {
		t.Fatalf("expected audience=[bob], got %v", env.Audience)
	}
}

// TestAsk_HappyPath_AutoResolveHandler covers the "未显式 --audience"
// path: when type_registry.handler_actor_id is set the CLI auto-fills
// audience from it (L2 §3.2.2 case 2).
func TestAsk_HappyPath_AutoResolveHandler(t *testing.T) {
	f := newBusFixture(t)
	f.installBizType(t, "xhs.publish",
		[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, "tool:xhs")

	exit, stdout, stderr := f.runCLI([]string{
		"ask", "--type", "xhs.publish",
		"--payload", `{"title":"t","content":"c"}`,
	}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	env, _ := f.store.FindByID(t.Context(), out.ID)
	if env == nil {
		t.Fatalf("envelope not stored")
	}
	if len(env.Audience) != 1 || env.Audience[0] != "tool:xhs" {
		t.Fatalf("expected auto-resolved audience=[tool:xhs], got %v", env.Audience)
	}
}

// TestAsk_AudienceInvalid_Wildcard is branch #1 of L2 §3.2.2:
// `--audience '*'` is non-concrete and must be rejected client-side
// as request_audience_invalid (no daemon roundtrip).
func TestAsk_AudienceInvalid_Wildcard(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "agent.text", "--audience", "*",
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessRequestAudienceInvalid)) {
		t.Fatalf("expected request_audience_invalid in stderr, got %q", stderr)
	}
	// Confirm no envelope was written — client-side reject.
	if len(f.store.byID) != 0 {
		t.Fatalf("client-side reject must not write any envelope; got %d", len(f.store.byID))
	}
}

// TestAsk_AudienceInvalid_MultiReceiver covers the length>1 branch:
// `--audience bob,carol` violates "exactly one concrete receiver".
func TestAsk_AudienceInvalid_MultiReceiver(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "agent.text", "--audience", "bob,carol",
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessRequestAudienceInvalid)) {
		t.Fatalf("expected request_audience_invalid in stderr, got %q", stderr)
	}
}

// TestAsk_AudienceInvalid_NoFallback covers the L2 §3.2.2 case 2
// failure: when --audience is omitted AND type has no
// handler_actor_id, CLI client-side rejects with a helpful hint.
func TestAsk_AudienceInvalid_NoFallback(t *testing.T) {
	f := newBusFixture(t)
	// Install a type without handler_actor_id.
	f.installBizType(t, "biz.open",
		[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, "")

	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "biz.open", "--payload", `{}`,
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, "request_audience_invalid") {
		t.Fatalf("expected request_audience_invalid in stderr, got %q", stderr)
	}
	if !strings.Contains(stderr, "pass --audience") {
		t.Fatalf("expected hint to pass --audience, got %q", stderr)
	}
}

// TestAsk_AudienceActorNotRegistered exercises branch #2: explicit
// audience targets an actor missing from actor_registry (or
// deregistered). Surfaces from the harness via Send.
func TestAsk_AudienceActorNotRegistered(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "agent.text", "--audience", "nobody",
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessAudienceActorNotRegistered)) {
		t.Fatalf("expected audience_actor_not_registered, got %q", stderr)
	}
}

// TestAsk_AudienceActorDeregistered also exercises branch #2: the
// audience actor is registered but has deregistered_at set.
func TestAsk_AudienceActorDeregistered(t *testing.T) {
	f := newBusFixture(t)
	f.actors.deregister("bob", fixedTimeMS-1000)

	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "agent.text", "--audience", "bob",
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessAudienceActorNotRegistered)) {
		t.Fatalf("expected audience_actor_not_registered, got %q", stderr)
	}
}

// TestAsk_AudienceHandlerMismatch exercises branch #3: type registry
// declares handler_actor_id="tool:xhs" but caller passes a different
// audience explicitly.
func TestAsk_AudienceHandlerMismatch(t *testing.T) {
	f := newBusFixture(t)
	f.installBizType(t, "xhs.publish",
		[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, "tool:xhs")

	exit, _, stderr := f.runCLI([]string{
		"ask", "--type", "xhs.publish",
		"--payload", `{}`,
		"--audience", "bob",
	}, nil)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessAudienceHandlerMismatch)) {
		t.Fatalf("expected audience_handler_mismatch, got %q", stderr)
	}
}

// TestAsk_MissingType reports a usage-style client error when --type
// is absent (ask is the only subcommand where --type is required).
func TestAsk_MissingType(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{"ask", "--audience", "bob", "hi"}, nil)
	if exit != exitFlagFormat {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitFlagFormat, exit, stderr)
	}
	if !strings.Contains(stderr, "--type") {
		t.Fatalf("expected stderr to mention --type, got %q", stderr)
	}
}
