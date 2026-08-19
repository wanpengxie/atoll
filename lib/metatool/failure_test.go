package metatool

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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

// What this pins: every error code an actor actually emits must have a known
// class here. Nothing else in the tree connects the two — the codes are bare
// string literals at ~40 Fail sites across five subsystems, and this table is
// hand-written.
//
// What breaks without it: an unclassified code falls through to the
// reason-based default, which for any actor that answered at all is
// "internal_error / inspect adapter logs before retrying". That is exactly the
// state this whole table was built to end, so a new code silently returns one
// call path to it, and nothing anywhere goes red. This is not hypothetical:
// mcp_cancelled was already missing when the table was first written, and was
// found only by grepping by hand.
//
// Why it is not a stronger check: it could be — make Fail take a typed code
// drawn from one closed set, and the compiler makes registration unavoidable.
// That means changing actorbase.Fail's signature and every call site, which is
// a decision the owner has not taken. Until then this scan is the only thing
// standing between a new code and a silent regression. Retire it the moment
// the typed set exists.
func TestEveryEmittedErrorCodeIsClassified(t *testing.T) {
	root := repoRoot(t)
	// The literal in `sys.Fail(msg, "some_code", ...)`, in any of the receiver
	// spellings used across the tree.
	site := regexp.MustCompile(`Fail\((?:msg|request|env|m)\b[^,]*,\s*"([a-z_]+)"`)
	emitted := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules", "docs", "atoll-site":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(root, path)
		for _, m := range site.FindAllStringSubmatch(string(body), -1) {
			if !slices.Contains(emitted[m[1]], rel) {
				emitted[m[1]] = append(emitted[m[1]], rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(emitted) < 10 {
		t.Fatalf("scan found only %d codes; the pattern has stopped matching real call sites", len(emitted))
	}
	for code, sites := range emitted {
		// internal_error is the fallback class itself, not a code needing one.
		if code == "internal_error" {
			continue
		}
		if _, known := classifyActorError(code); !known {
			t.Errorf("error code %q is emitted (%s) but has no class in actorErrorClasses; add one so agents get a usable code, hint and retryable verdict instead of the internal_error fallback",
				code, strings.Join(sites, ", "))
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test's working directory")
		}
		dir = parent
	}
}
