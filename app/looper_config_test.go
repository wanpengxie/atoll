package app

import (
	"encoding/json"
	"testing"
)

// TestWithLooperRejectsNonObject pins the codex-Major fix: a non-object agent
// config is a HARD error (no silent coercion that drops the payload), while an
// object / empty / null config packs the looper key cleanly.
func TestWithLooperRejectsNonObject(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  string
		ok   bool
	}{
		{"object", `{"model":"x"}`, true},
		{"empty-object", `{}`, true},
		{"empty", ``, true},
		{"null", `null`, true}, // treated as empty
		{"array", `[1,2,3]`, false},
		{"string", `"hi"`, false},
		{"number", `42`, false},
		{"bool", `true`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := withLooper(json.RawMessage(tc.cfg), "claude")
			if tc.ok {
				if err != nil {
					t.Fatalf("want ok, got err: %v", err)
				}
				var m map[string]json.RawMessage
				if e := json.Unmarshal(out, &m); e != nil {
					t.Fatalf("output not a JSON object: %v", e)
				}
				if string(m["looper"]) != `"claude"` {
					t.Fatalf("looper key not packed: %s", out)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error for non-object %q, got output %s", tc.cfg, out)
			}
			if out != nil {
				t.Fatalf("want nil output on error, got %s", out)
			}
		})
	}
}

// TestWithLooperPreservesObjectPayload ensures the original object keys survive
// the looper repack (the regression codex flagged: payload silently dropped).
func TestWithLooperPreservesObjectPayload(t *testing.T) {
	out, err := withLooper(json.RawMessage(`{"model":"sonnet","temp":1}`), "claude")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if e := json.Unmarshal(out, &m); e != nil {
		t.Fatalf("output not an object: %v", e)
	}
	if string(m["model"]) != `"sonnet"` || string(m["temp"]) != `1` {
		t.Fatalf("original payload not preserved: %s", out)
	}
	if string(m["looper"]) != `"claude"` {
		t.Fatalf("looper not added: %s", out)
	}
}

// TestIsJSONObject pins the API-entry guard: only a real JSON object passes;
// null / array / scalar are rejected (a present-but-non-object config is 400).
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
