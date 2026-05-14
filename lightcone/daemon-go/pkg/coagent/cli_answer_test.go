package coagent

import (
	"encoding/json"
	"strings"
	"testing"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// seedAnswerRequest plants a request envelope so `coagent answer`
// can look it up via the binding's LookupRequest path. The request
// is from bob to alice; alice's answer therefore loops back to bob.
func seedAnswerRequest(f *busFixture, t *testing.T, typeName, handlerActor string) string {
	t.Helper()
	if typeName != "agent.text" {
		f.installBizType(t, typeName,
			[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, handlerActor)
	}
	req := &v4types.Envelope{
		ID:            "req-x",
		TS:            fixedTimeMS - 1000,
		ChannelID:     "ch-1",
		Sender:        v4types.Sender{Kind: v4types.SenderAgent, ID: "bob"},
		Kind:          v4types.KindRequest,
		Type:          typeName,
		Payload:       json.RawMessage(`{"q":"status?"}`),
		CorrelationID: "chain-orig",
		Visibility:    v4types.VisibilityPublic,
		Audience:      []string{"alice"},
	}
	f.store.seedRequest(req)
	return req.ID
}

// TestAnswer_HappyPath_AutoFillFromRequest verifies L2 §3.4.3:
// answer auto-fills parent_id / correlation_id / audience / type from
// the looked-up request.
func TestAnswer_HappyPath_AutoFillFromRequest(t *testing.T) {
	f := newBusFixture(t)
	reqID := seedAnswerRequest(f, t, "agent.text", "")

	exit, stdout, stderr := f.runCLI([]string{
		"answer", reqID, "done, see work/x.md",
	}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.Kind != string(v4types.KindResponse) {
		t.Fatalf("expected kind=response, got %q", out.Kind)
	}
	if out.CorrelationID != "chain-orig" {
		t.Fatalf("expected correlation_id inherited (chain-orig), got %q", out.CorrelationID)
	}

	env, _ := f.store.FindByID(t.Context(), out.ID)
	if env == nil {
		t.Fatalf("envelope not stored")
	}
	if env.ParentID != reqID {
		t.Fatalf("expected parent_id=%q, got %q", reqID, env.ParentID)
	}
	if env.Type != "agent.text" {
		t.Fatalf("expected type inherited from request, got %q", env.Type)
	}
	if len(env.Audience) != 1 || env.Audience[0] != "bob" {
		t.Fatalf("expected audience=[bob] (request sender), got %v", env.Audience)
	}
}

// TestAnswer_OverrideType lets caller change type via --type while
// keeping parent_id / audience / correlation_id auto-filled.
func TestAnswer_OverrideType(t *testing.T) {
	f := newBusFixture(t)
	// seed a biz.foo request so the override has a real registered
	// type to land on.
	f.installBizType(t, "biz.foo",
		[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, "")
	f.installBizType(t, "biz.bar",
		[]v4types.Kind{v4types.KindRequest, v4types.KindResponse}, "")
	req := &v4types.Envelope{
		ID:            "req-y",
		TS:            fixedTimeMS - 500,
		ChannelID:     "ch-1",
		Sender:        v4types.Sender{Kind: v4types.SenderAgent, ID: "bob"},
		Kind:          v4types.KindRequest,
		Type:          "biz.foo",
		Payload:       json.RawMessage(`{}`),
		CorrelationID: "chain-z",
		Visibility:    v4types.VisibilityPublic,
		Audience:      []string{"alice"},
	}
	f.store.seedRequest(req)

	exit, stdout, stderr := f.runCLI([]string{
		"answer", "req-y", "--type", "biz.bar", "--payload", `{"ok":true}`,
	}, nil)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	env, _ := f.store.FindByID(t.Context(), out.ID)
	if env == nil {
		t.Fatalf("envelope not stored")
	}
	if env.Type != "biz.bar" {
		t.Fatalf("expected type=biz.bar (override), got %q", env.Type)
	}
	if env.CorrelationID != "chain-z" {
		t.Fatalf("expected correlation_id auto-fill (chain-z), got %q", env.CorrelationID)
	}
}

// TestAnswer_MissingRequestID emits a usage error.
func TestAnswer_MissingRequestID(t *testing.T) {
	f := newBusFixture(t)
	exit, _, stderr := f.runCLI([]string{"answer"}, nil)
	if exit != exitUsage {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitUsage, exit, stderr)
	}
	if !strings.Contains(stderr, "request_id") {
		t.Fatalf("expected stderr to mention request_id, got %q", stderr)
	}
}

// TestAnswer_NoLookupBinding tests the daemon_rpc-style binding where
// LookupRequest returns (nil,false,nil). The CLI then requires --type
// + --audience explicitly. We use a custom binding stub here.
func TestAnswer_NoLookupBinding_RequiresType(t *testing.T) {
	stubB := &stubBinding{
		send: func(_ pkgharness.Deps) (*SendResult, error) {
			return &SendResult{ID: "out-1", CorrelationID: "out-1", Kind: v4types.KindResponse}, nil
		},
	}
	exit, _, stderr := runWithBinding([]string{"answer", "req-z", "hello"}, stubB)
	if exit != exitFlagFormat {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitFlagFormat, exit, stderr)
	}
	if !strings.Contains(stderr, "--type required") {
		t.Fatalf("expected stderr to require --type, got %q", stderr)
	}
}
