package base

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

// The control words do NOT take an origin, and that is deliberate rather than
// an omission: an agent told to stop stops the same way wherever the button was
// pressed. Pinned so nobody widens it back out by reflex.
func TestControlWordsDoNotTakeAnOrigin(t *testing.T) {
	for _, body := range []string{
		`{"origin":{"session":"s-abc"}}`,
		`{"target":"r1","origin":{"session":"s-abc"}}`,
	} {
		var interrupt struct{}
		if err := decodeStrict(json.RawMessage(body), &interrupt); err == nil {
			t.Fatalf("a control word accepted %s; only the sentence needs a place to have come from", body)
		}
	}
}

// A person's sentence came from one of their screens, and that screen has an
// identity. It rides in agent.ask's own body — not in _context, which every
// actor carries and where a human-only fact would mean nothing to anyone else,
// and not on the control words, which do the same thing wherever the button was
// pressed.
//
// On 2026-08-28 agent.ask did not accept it and the first real message through
// the build died on `json: unknown field "origin"` — an error with no visible
// relation to the feature that caused it.

// The ask struct is the one that actually broke, so it is decoded literally.
func TestAskAcceptsAnOriginAndStillRequiresItsText(t *testing.T) {
	var ask struct {
		Text        string            `json:"text"`
		Attachments []json.RawMessage `json:"attachments,omitempty"`
		Origin      *originSpec       `json:"origin,omitempty"`
	}
	if err := decodeStrict(json.RawMessage(`{"text":"hi","origin":{"session":"s-abc","label":"Mac Chrome"}}`), &ask); err != nil {
		t.Fatalf("agent.ask rejected an origin: %v", err)
	}
	if ask.Origin == nil || ask.Origin.Session != "s-abc" || ask.Origin.Label != "Mac Chrome" {
		t.Fatalf("origin=%+v, want it decoded whole", ask.Origin)
	}
	// Still strict about everything else: this fix must not become the removal
	// of the check.
	if err := decodeStrict(json.RawMessage(`{"text":"hi","bogus":1}`), &ask); err == nil {
		t.Fatal("an unknown field was accepted; the strictness is the point")
	}
	// And a message with no origin is ordinary, not an error — an agent may be
	// addressed by something that has no screen at all.
	ask.Origin = nil
	if err := decodeStrict(json.RawMessage(`{"text":"hi"}`), &ask); err != nil || ask.Origin != nil {
		t.Fatalf("a message without an origin should decode cleanly: err=%v origin=%+v", err, ask.Origin)
	}
}

// The session id is reproduced exactly because an agent sends it BACK: it is
// what a ui.* word takes to say which screen to operate. A prettier rendering
// that mangled it would break the only thing it is for.
func TestTheOriginLineCarriesTheIdBackVerbatim(t *testing.T) {
	line := driverproto.OriginLine(&driverproto.Origin{Session: "s-8df126b3-b0d4", Label: "Mac Chrome"})
	if !strings.Contains(line, "s-8df126b3-b0d4") {
		t.Fatalf("line=%q, want the session id reproduced exactly", line)
	}
	if !strings.Contains(line, "Mac Chrome") {
		t.Fatalf("line=%q, want the label a person can recognise", line)
	}
	// Nothing to say is said with nothing: an empty [origin] line would look
	// like an answer.
	if got := driverproto.OriginLine(nil); got != "" {
		t.Fatalf("OriginLine(nil)=%q, want empty", got)
	}
	if got := driverproto.OriginLine(&driverproto.Origin{Label: "no id"}); got != "" {
		t.Fatalf("OriginLine without a session=%q, want empty", got)
	}
}
