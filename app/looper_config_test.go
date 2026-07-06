package app

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestIsJSONObject pins the API-entry guard: only a real JSON object passes;
// null / array / scalar are rejected (a present-but-non-object agent config is
// 400). Config is the opaque persona/knobs container; the engine is NOT in it
// (engine = the actor class).
func TestIsJSONObject(t *testing.T) {
	cases := map[string]bool{
		`{}`:      true,
		`{"a":1}`: true,
		`null`:    false,
		`[1]`:     false,
		`"s"`:     false,
		`42`:      false,
		`true`:    false,
	}
	for raw, want := range cases {
		if got := isJSONObject(json.RawMessage(raw)); got != want {
			t.Errorf("isJSONObject(%s) = %v, want %v", raw, got, want)
		}
	}
}

// TestMergeConfig_DeepPoisonNotRecursed pins the JSON-depth guard (#1) on the
// persisted-config path: a config nested past maxJSONDepth would fatally overflow the
// goroutine stack when mergeConfig unmarshals it into map[string]any on EVERY startup
// (loadChannels → reconcile build → mergeConfig). The bounded-depth pre-check refuses
// to treat an over-deep layer as an object (never recursing into it) — the call must
// RETURN, not crash.
func TestMergeConfig_DeepPoisonNotRecursed(t *testing.T) {
	poison := strings.Repeat("[", 20000) + strings.Repeat("]", 20000)

	// Poison as the per-channel layer: never recursed; returned verbatim as raw bytes
	// (the looper self-parses opaquely in its own subprocess).
	if out := mergeConfig("", poison); string(out) != poison {
		t.Fatalf("deep per-channel config not preserved verbatim (got len %d, want %d)", len(out), len(poison))
	}
	// Poison as the global layer WITH a valid per-channel object: the poison global is
	// skipped (not an object → no merge), the valid per-channel layer is preserved.
	if out := mergeConfig(poison, `{"model":"x"}`); string(out) != `{"model":"x"}` {
		t.Fatalf("valid per-channel layer lost when global was poison: %s", out)
	}
	// Happy path intact: two valid object layers still shallow-merge (channel wins).
	out := mergeConfig(`{"a":1,"model":"g"}`, `{"model":"c"}`)
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("valid two-layer merge produced invalid JSON: %s (%v)", out, err)
	}
	if m["a"] == nil || m["model"] != "c" {
		t.Fatalf("two-layer merge wrong: %v (want a preserved, model=c)", m)
	}
}

// TestBoundedJSONDepth pins the app-side depth gate directly.
func TestBoundedJSONDepth(t *testing.T) {
	over := strings.Repeat("[", maxJSONDepth+1) + strings.Repeat("]", maxJSONDepth+1)
	if err := boundedJSONDepth([]byte(over)); err == nil {
		t.Fatal("boundedJSONDepth accepted an over-deep blob")
	}
	if err := boundedJSONDepth([]byte(`{"a":{"b":1}}`)); err != nil {
		t.Fatalf("boundedJSONDepth rejected a shallow blob: %v", err)
	}
}
