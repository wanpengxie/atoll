package xhs

import (
	"testing"

	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestTypeClosedSet asserts AllTypes is 1:1 with the L4 §2.1 closed set.
func TestTypeClosedSet(t *testing.T) {
	want := map[string]bool{
		TypePublish:      true,
		TypeSearch:       true,
		TypeNoteFetch:    true,
		TypeRecentFetch:  true,
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
	for _, ty := range []string{TypeSearch, TypeNoteFetch, TypeRecentFetch} {
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
	for _, ty := range []string{TypeSearch, TypeNoteFetch, TypeRecentFetch} {
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

// TestDeclarationTypeDeclarationsCoversEveryType asserts the
// invariant: every entry of AllTypes has a matching TypeDeclaration
// row. Without this the framework's strict-mode install would reject
// the declaration with InstallTypeRegistryInvalid.
//
// Level A (proto-layer0 §1.4.1): TypeDeclaration carries only
// allowed_kinds + terminal_convention — payload schemas are NOT part
// of the protocol layer.
func TestDeclarationTypeDeclarationsCoversEveryType(t *testing.T) {
	decls := DeclarationTypeDeclarations()
	for _, ty := range AllTypes {
		if _, ok := decls[ty]; !ok {
			t.Errorf("DeclarationTypeDeclarations missing entry for %q", ty)
		}
	}
	if len(decls) != len(AllTypes) {
		t.Errorf("DeclarationTypeDeclarations count=%d want %d (one entry per AllTypes member)",
			len(decls), len(AllTypes))
	}
}

// TestTypeDeclarationsAllowedKindsSpec asserts each xhs type's
// AllowedKinds matches domain-xhs-spec §1.1–§1.6:
//   - R/R types (publish/search/note.fetch/recent.fetch/cookie.sync)
//     → {request, response}
//   - event-only (note.archived) → {event}
func TestTypeDeclarationsAllowedKindsSpec(t *testing.T) {
	decls := DeclarationTypeDeclarations()
	rr := []string{TypePublish, TypeSearch, TypeNoteFetch, TypeRecentFetch}
	for _, ty := range rr {
		got := decls[ty].AllowedKinds
		if len(got) != 2 {
			t.Errorf("%s: allowed_kinds=%v want 2 entries (request, response)", ty, got)
			continue
		}
		seen := map[string]bool{}
		for _, k := range got {
			seen[string(k)] = true
		}
		if !seen[string(message.KindRequest)] || !seen[string(message.KindResponse)] {
			t.Errorf("%s: allowed_kinds=%v want {request, response}", ty, got)
		}
	}
	ev := decls[TypeNoteArchived].AllowedKinds
	if len(ev) != 1 || ev[0] != message.KindEvent {
		t.Errorf("%s: allowed_kinds=%v want [event]", TypeNoteArchived, ev)
	}
}

// TestTypeDeclarationsInstallValidates exercises the framework's
// install-time validation against the xhs DeclarationTypeDeclarations.
// Any drift in the closed-set rules (allowed_kinds empty / unknown
// kind / bad terminal_convention) trips here.
func TestTypeDeclarationsInstallValidates(t *testing.T) {
	decls := DeclarationTypeDeclarations()
	for ty, td := range decls {
		if err := framework.ValidateTypeDeclaration(ty, td); err != nil {
			t.Errorf("ValidateTypeDeclaration(%s) failed: %v", ty, err)
		}
	}
}

// TestTypeDeclarationsRejectsDisallowedKind asserts the harness Step
// 5 kind allow-list against domain-xhs-spec §1.x: pushing a kind=event
// envelope at an R/R-only type (e.g. xhs.publish) MUST be rejected.
// This is the regression guard for "harness accepts spec-disallowed
// kind".
func TestTypeDeclarationsRejectsDisallowedKind(t *testing.T) {
	decls := DeclarationTypeDeclarations()
	publish := decls[TypePublish]
	for _, k := range publish.AllowedKinds {
		if k == message.KindEvent {
			t.Fatalf("xhs.publish should NOT include kind=event in AllowedKinds; got %v",
				publish.AllowedKinds)
		}
	}
	archived := decls[TypeNoteArchived]
	for _, k := range archived.AllowedKinds {
		if k == message.KindRequest || k == message.KindResponse {
			t.Fatalf("xhs.note.archived must be event-only; AllowedKinds=%v leaks %s",
				archived.AllowedKinds, k)
		}
	}
}
