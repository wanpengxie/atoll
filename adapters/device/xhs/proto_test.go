package xhs

import "testing"

// TestTypeClosedSet asserts AllTypes is 1:1 with the L4 §2.1 closed set.
func TestTypeClosedSet(t *testing.T) {
	want := map[string]bool{
		TypePublish:      true,
		TypeSearch:       true,
		TypeNoteFetch:    true,
		TypeRecentFetch:  true,
		TypeCookieSync:   true,
		TypeNoteArchived: true,
	}
	if len(AllTypes) != len(want) {
		t.Fatalf("AllTypes length %d != want %d", len(AllTypes), len(want))
	}
	for _, ty := range AllTypes {
		if !want[ty] {
			t.Errorf("AllTypes contains unexpected %q", ty)
		}
	}
}

// TestRequestResponseSubset assert RequestResponseTypes excludes the
// event-only TypeNoteArchived.
func TestRequestResponseSubset(t *testing.T) {
	for _, ty := range RequestResponseTypes {
		if ty == TypeNoteArchived {
			t.Errorf("RequestResponseTypes should not include event-only %q", ty)
		}
	}
	if len(RequestResponseTypes) != len(AllTypes)-1 {
		t.Errorf("expected RequestResponseTypes to be AllTypes - 1 (one event-only)")
	}
}

// TestAllowListShapes guards the per-type allow-list invariants:
//   - publish allows device_id; the other R/R types do NOT
//   - only the 5 R/R types have entries; xhs.note.archived has none
func TestAllowListShapes(t *testing.T) {
	if _, ok := allowedResultKeysByType[TypePublish]["device_id"]; !ok {
		t.Error("publish result allow-list should contain device_id")
	}
	for _, ty := range []string{TypeSearch, TypeNoteFetch, TypeRecentFetch, TypeCookieSync} {
		if _, ok := allowedResultKeysByType[ty]["device_id"]; ok {
			t.Errorf("%s result allow-list must NOT contain device_id (R4-FIX-A)", ty)
		}
	}
	if _, ok := allowedResultKeysByType[TypeNoteArchived]; ok {
		t.Error("event-only type must not appear in result allow-list")
	}

	if _, ok := allowedErrorKeysByType[TypePublish]["device_id"]; !ok {
		t.Error("publish error allow-list should contain device_id")
	}
	for _, ty := range []string{TypeSearch, TypeNoteFetch, TypeRecentFetch, TypeCookieSync} {
		if _, ok := allowedErrorKeysByType[ty]["device_id"]; ok {
			t.Errorf("%s error allow-list must NOT contain device_id (R4-FIX-A)", ty)
		}
	}
}

// TestUnknownTypeAllowListsEmpty makes sure a recovered legacy / drifted
// request type falls through to the empty allow-list (R4-FIX-A
// fail-closed default).
func TestUnknownTypeAllowListsEmpty(t *testing.T) {
	if got := resultAllowListFor(""); len(got) != 0 {
		t.Errorf("blank type should yield empty result allow-list; got %v", got)
	}
	if got := resultAllowListFor("xhs.future"); len(got) != 0 {
		t.Errorf("unknown type should yield empty result allow-list; got %v", got)
	}
	if got := errorAllowListFor(""); len(got) != 0 {
		t.Errorf("blank type should yield empty error allow-list; got %v", got)
	}
}

// TestCommandWireTypeStable guards against the wire-format constant
// drift — extensions in the field rely on Command.Type == "command".
func TestCommandWireTypeStable(t *testing.T) {
	if CommandWireType != "command" {
		t.Errorf("CommandWireType drift: got %q want \"command\"", CommandWireType)
	}
}
