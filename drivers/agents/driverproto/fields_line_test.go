package driverproto

import (
	"encoding/json"
	"strings"
	"testing"
)

// An agent used to see only body.text, so a word could not grow a parameter the
// agent was meant to act on. These pin the fix: everything else in the body
// reaches the agent, exactly as sent.

func TestABodyOfOnlyTextRendersNothingExtra(t *testing.T) {
	if got := FieldsLine(json.RawMessage(`{"text":"hello"}`)); got != "" {
		t.Fatalf("FieldsLine=%q, want empty — there is nothing besides the text", got)
	}
}

// The point of the change: a field an agent has to send back must arrive
// verbatim, because it will be quoted into the next request.
func TestTheOtherFieldsReachTheAgentExactly(t *testing.T) {
	got := FieldsLine(json.RawMessage(`{"text":"帮我切到 c0","origin":{"session":"s-7f3a","label":"MacBook 网页"}}`))
	if !strings.Contains(got, `origin={"session":"s-7f3a","label":"MacBook 网页"}`) {
		t.Fatalf("FieldsLine=%q, want the origin object reproduced exactly", got)
	}
	if strings.Contains(got, "帮我切到") {
		t.Fatalf("FieldsLine=%q, want the person's words left to the text", got)
	}
}

// Same body, same rendering, every time: an agent re-reading its own context
// must not meet two spellings of one message.
func TestRenderingIsStable(t *testing.T) {
	body := json.RawMessage(`{"b":2,"text":"x","a":1,"c":3}`)
	first := FieldsLine(body)
	for i := 0; i < 5; i++ {
		if got := FieldsLine(body); got != first {
			t.Fatalf("rendering changed between calls: %q then %q", first, got)
		}
	}
	if !strings.Contains(first, "[fields a=1 b=2 c=3]") {
		t.Fatalf("FieldsLine=%q, want the fields in a fixed order", first)
	}
}

// A body that is not an object, or is absent, must not produce noise: Text
// already carried whatever it was.
func TestNonObjectBodiesRenderNothing(t *testing.T) {
	for _, body := range []string{``, `"just a string"`, `[1,2,3]`, `null`, `not json at all`} {
		if got := FieldsLine(json.RawMessage(body)); got != "" {
			t.Errorf("FieldsLine(%q)=%q, want empty", body, got)
		}
	}
}

// Nothing structurally bounds a body, and a prompt is not the place to discover
// that. Truncation is announced, so an agent knows the line is not the whole
// story rather than trusting an object that stops mid-key.
func TestAnOversizedBodyIsTruncatedOutLoud(t *testing.T) {
	big, _ := json.Marshal(map[string]string{"text": "hi", "blob": strings.Repeat("x", 8000)})
	got := FieldsLine(big)
	if len(got) > maxFieldsLine+32 {
		t.Fatalf("rendered %d bytes; the cap is meant to bound the prompt", len(got))
	}
	if !strings.HasSuffix(got, "…truncated]") {
		t.Fatalf("FieldsLine=%q, want the truncation said out loud", got[max(0, len(got)-40):])
	}
}
