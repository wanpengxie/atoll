package message

import "testing"

// TestParseKind pins the Kind closed set {event,request,response}: every
// in-set wire form resolves and round-trips its string; every out-of-set
// value (including empty and casing variants) is rejected with ok=false
// and an empty Kind, so an illegal value can never enter the ADT.
func TestParseKind(t *testing.T) {
	t.Parallel()
	valid := []Kind{KindEvent, KindRequest, KindResponse}
	for _, k := range valid {
		got, ok := ParseKind(string(k))
		if !ok {
			t.Errorf("ParseKind(%q) ok=false, want true", k)
		}
		if got != k {
			t.Errorf("ParseKind(%q) = %q, want %q", k, got, k)
		}
		if got.String() != string(k) {
			t.Errorf("Kind(%q).String() = %q", k, got.String())
		}
	}

	invalid := []string{
		"",
		"Event",  // wrong case
		"EVENT",  // wrong case
		"events", // close-but-no
		"reply",  // not in set
		"notification",
		"event ", // trailing space
		" event", // leading space
	}
	for _, raw := range invalid {
		got, ok := ParseKind(raw)
		if ok {
			t.Errorf("ParseKind(%q) ok=true, want false (out-of-set must reject)", raw)
		}
		if got != "" {
			t.Errorf("ParseKind(%q) returned non-empty Kind %q on reject", raw, got)
		}
	}
}

// TestParseVisibility pins the Visibility closed set
// {public,system}: in-set resolves and round-trips, out-of-set
// rejects with empty Visibility.
func TestParseVisibility(t *testing.T) {
	t.Parallel()
	valid := []Visibility{VisibilityPublic, VisibilitySystem}
	for _, v := range valid {
		got, ok := ParseVisibility(string(v))
		if !ok {
			t.Errorf("ParseVisibility(%q) ok=false, want true", v)
		}
		if got != v {
			t.Errorf("ParseVisibility(%q) = %q, want %q", v, got, v)
		}
		if got.String() != string(v) {
			t.Errorf("Visibility(%q).String() = %q", v, got.String())
		}
	}

	invalid := []string{
		"",
		"private", // trust boundaries are channels, not per-message ACLs
		"Public",
		"PUBLIC",
		"protected", // not in set
		"internal",  // not in set
		"hidden",
		"public ",
	}
	for _, raw := range invalid {
		got, ok := ParseVisibility(raw)
		if ok {
			t.Errorf("ParseVisibility(%q) ok=true, want false", raw)
		}
		if got != "" {
			t.Errorf("ParseVisibility(%q) returned non-empty %q on reject", raw, got)
		}
	}
}
