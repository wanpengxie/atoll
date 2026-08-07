package access

import "testing"

// TestParseOperation pins the Operation closed set {create,read,write,delete}:
// every in-set wire form resolves and round-trips its string; every out-of-set
// value (empty, casing variants, near-misses, not-in-set verbs, whitespace) is
// rejected with ok=false AND an empty Operation, so an illegal value can never
// enter the ADT via a bare cast.
func TestParseOperation(t *testing.T) {
	t.Parallel()
	valid := []Operation{OpCreate, OpRead, OpWrite, OpDelete}
	for _, op := range valid {
		got, ok := ParseOperation(string(op))
		if !ok {
			t.Errorf("ParseOperation(%q) ok=false, want true", op)
		}
		if got != op {
			t.Errorf("ParseOperation(%q) = %q, want %q", op, got, op)
		}
		if got.String() != string(op) {
			t.Errorf("Operation(%q).String() = %q", op, got.String())
		}
	}

	invalid := []string{
		"",
		"Create",   // wrong case
		"CREATE",   // wrong case
		"creates",  // close-but-no
		"updates",  // close-but-no
		"use",      // known-additive but NOT in day-1 set
		"set",      // retired with the grants plane (PM-D1): no per-object grant verb
		"transfer", // not an op
		"list",     // registry/obs plane, not access
		"create ",  // trailing space
		" create",  // leading space
	}
	for _, raw := range invalid {
		got, ok := ParseOperation(raw)
		if ok {
			t.Errorf("ParseOperation(%q) ok=true, want false (out-of-set must reject)", raw)
		}
		if got != "" {
			t.Errorf("ParseOperation(%q) returned non-empty Operation %q on reject", raw, got)
		}
	}
}

// TestOperationSetSize pins the closed set at exactly 4 members. The set falls
// out of the resource lifecycle and is frozen: adding a verb is a
// protocol revision, so this count is a deliberate drift tripwire. It asserts on
// the unexported backing slice allOperations directly (not a re-listed literal),
// so a 5th constant wired into the const block + allOperations trips this test —
// forcing the author to acknowledge a protocol change here.
func TestOperationSetSize(t *testing.T) {
	t.Parallel()
	if len(allOperations) != 4 {
		t.Fatalf("Operation closed set drifted: allOperations has %d members, want 4 — adding/removing a verb is a protocol revision; update this sentinel deliberately", len(allOperations))
	}
	for _, o := range allOperations {
		if _, ok := ParseOperation(string(o)); !ok {
			t.Errorf("backing-slice member %q not accepted by ParseOperation", o)
		}
	}
}
