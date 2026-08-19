package actor

import "testing"

// ParseKind is the substrate's ONE deserialization gate for the actor-kind
// closed set: every wire frame and every truth-store row scan that turns a
// string back into a Kind goes through it instead of a bare actor.Kind(string)
// cast. Its whole value is therefore the pair of closures below — what it lets
// in, and what it keeps out. Neither half was pinned anywhere before this file.

// TestParseKindAcceptsTheClosedSet is the ACCEPT half: the five canonical wire
// forms resolve, each to its own constant, and the value that comes back
// re-serializes to the exact string that produced it (so a row written by one
// process and read by another cannot drift).
func TestParseKindAcceptsTheClosedSet(t *testing.T) {
	accepted := map[string]Kind{
		"human":  KindHuman,
		"agent":  KindAgent,
		"peer":   KindPeer,
		"system": KindSystem,
		"tool":   KindTool,
	}
	for raw, want := range accepted {
		got, ok := ParseKind(raw)
		if !ok {
			t.Fatalf("ParseKind(%q) rejected a member of the closed set", raw)
		}
		if got != want {
			t.Fatalf("ParseKind(%q) = %q, want %q", raw, got, want)
		}
		if got.String() != raw {
			t.Fatalf("ParseKind(%q).String() = %q — wire form is not round-trip stable", raw, got.String())
		}
		if again, ok := ParseKind(got.String()); !ok || again != want {
			t.Fatalf("re-parsing %q's own wire form gave (%q,%v)", want, again, ok)
		}
	}
	// The set is CLOSED, not merely non-empty: an added constant that no
	// caller expects is as much a contract break as a removed one, and the
	// enumeration ParseKind walks is the only place that could grow.
	if len(allKinds) != len(accepted) {
		t.Fatalf("closed set size = %d, want exactly %d (%v)", len(allKinds), len(accepted), allKinds)
	}
	for _, k := range allKinds {
		if _, listed := accepted[string(k)]; !listed {
			t.Fatalf("closed set grew a member this contract does not name: %q", k)
		}
	}
}

// TestParseKindRejectsOutOfSetInput is the REJECT half. Every case is a shape a
// real caller could hand it — a hand-edited DB row, a case-normalizing client, a
// trimmed-wrong field, a truncated read, a foreign vocabulary — and on all of
// them ParseKind must return the ZERO Kind together with ok=false. The zero
// value matters as much as the bool: a caller that ignores ok must still not
// end up with a plausible-looking Kind in hand.
func TestParseKindRejectsOutOfSetInput(t *testing.T) {
	rejected := []struct {
		name string
		raw  string
	}{
		{"empty string", ""},
		{"capitalized", "Human"},
		{"upper case", "AGENT"},
		{"leading space", " system"},
		{"trailing space", "tool "},
		{"trailing newline", "human\n"},
		{"embedded NUL", "human\x00"},
		{"prefix of a member", "hum"},
		{"member plus suffix", "humans"},
		{"two members concatenated", "human,agent"},
		{"fullwidth homoglyphs", "ｈｕｍａｎ"},
		{"foreign vocabulary", "bot"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseKind(tc.raw)
			if ok {
				t.Fatalf("ParseKind(%q) admitted an out-of-set value as %q", tc.raw, got)
			}
			if got != "" {
				t.Fatalf("ParseKind(%q) rejected but still returned %q — a rejected parse must yield the zero Kind", tc.raw, got)
			}
		})
	}
}

// TestParseKindRejectsTheZeroKindItself closes the loop the two halves leave
// open: Kind("")'s own wire form must not be parseable, or "field was never
// set" and "field says human" would become the same state after one
// serialize/deserialize round trip.
func TestParseKindRejectsTheZeroKindItself(t *testing.T) {
	var zero Kind
	if _, ok := ParseKind(zero.String()); ok {
		t.Fatal("the zero Kind's wire form parses — an unset field would deserialize into a real kind")
	}
}
