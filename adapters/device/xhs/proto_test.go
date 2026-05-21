package xhs

import (
	"testing"

	"github.com/wanpengxie/ActOS/adapters/framework"
)

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

// TestDeclarationTypeSchemasCoversEveryType asserts the R5-18
// invariant: every entry of AllTypes has a matching TypeSchema row.
// Without this the framework's strict-mode install would reject the
// declaration with InstallTypeRegistryInvalid.
func TestDeclarationTypeSchemasCoversEveryType(t *testing.T) {
	schemas := DeclarationTypeSchemas()
	for _, ty := range AllTypes {
		if _, ok := schemas[ty]; !ok {
			t.Errorf("DeclarationTypeSchemas missing entry for %q (R5-18)", ty)
		}
	}
	if len(schemas) != len(AllTypes) {
		t.Errorf("DeclarationTypeSchemas count=%d want %d (one entry per AllTypes member)",
			len(schemas), len(AllTypes))
	}
}

// TestTypeSchemasAllowedKindsSpec asserts each xhs type's
// AllowedKinds matches domain-xhs-spec §1.1–§1.6:
//   - R/R types (publish/search/note.fetch/recent.fetch/cookie.sync)
//     → {request, response}
//   - event-only (note.archived) → {event}
func TestTypeSchemasAllowedKindsSpec(t *testing.T) {
	schemas := DeclarationTypeSchemas()
	rr := []string{TypePublish, TypeSearch, TypeNoteFetch, TypeRecentFetch, TypeCookieSync}
	for _, ty := range rr {
		got := schemas[ty].AllowedKinds
		if len(got) != 2 {
			t.Errorf("%s: allowed_kinds=%v want 2 entries (request, response)", ty, got)
			continue
		}
		seen := map[string]bool{}
		for _, k := range got {
			seen[string(k)] = true
		}
		if !seen["request"] || !seen["response"] {
			t.Errorf("%s: allowed_kinds=%v want {request, response}", ty, got)
		}
	}
	ev := schemas[TypeNoteArchived].AllowedKinds
	if len(ev) != 1 || string(ev[0]) != "event" {
		t.Errorf("%s: allowed_kinds=%v want [event]", TypeNoteArchived, ev)
	}
}

// TestTypeSchemasInstallValidates exercises the framework's install-
// time schema validation against the xhs DeclarationTypeSchemas. Any
// drift in the closed-set rules (allowed_kinds empty / unknown kind /
// schemas_by_kind out of allow-list / fallback schema rejects spec
// terminal payloads) trips here.
//
// The R5-18 motivation is that this validation must pass at install
// time for every entry — if it fails for any type, that type cannot
// install per the fail-closed policy.
func TestTypeSchemasInstallValidates(t *testing.T) {
	schemas := DeclarationTypeSchemas()
	for ty, ts := range schemas {
		if err := framework.ValidateTypeSchema(ty, ts); err != nil {
			t.Errorf("ValidateTypeSchema(%s) failed: %v", ty, err)
		}
	}
}

// TestTypeSchemasRejectsDisallowedKind exercises the harness Step 4/5
// kind allow-list against domain-xhs-spec §1.x: pushing a kind=event
// envelope at an R/R-only type (e.g. xhs.publish) MUST be rejected.
// This is the regression guard for the original R5-18 violation —
// "harness accepts spec-disallowed kind".
func TestTypeSchemasRejectsDisallowedKind(t *testing.T) {
	schemas := DeclarationTypeSchemas()
	publish := schemas[TypePublish]
	for _, k := range publish.AllowedKinds {
		if string(k) == "event" {
			t.Fatalf("xhs.publish should NOT include kind=event in AllowedKinds; got %v",
				publish.AllowedKinds)
		}
	}
	archived := schemas[TypeNoteArchived]
	for _, k := range archived.AllowedKinds {
		if string(k) == "request" || string(k) == "response" {
			t.Fatalf("xhs.note.archived must be event-only; AllowedKinds=%v leaks %s",
				archived.AllowedKinds, k)
		}
	}
}
