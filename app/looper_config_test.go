package app

import (
	"encoding/json"
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
