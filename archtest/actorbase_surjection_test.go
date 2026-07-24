package archtest

import (
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// TestActorbaseSysIsSurjectiveOverCapabilityFace is actorbase-spec-v1.md §1.2 /
// §5 DoD③'s "能力语义清单满射断言": the surjection target is the substrate's
// closed capability-face SEMANTIC ATOMS, NOT its Go method count — a Go
// method-set grep would falsely pass on a single-method AccessHandle.Invoke
// or harness.Pen.Write even if the underlying atoms (access.Operation values /
// message Kind×status) were not individually reachable from Sys. This test
// enumerates each atom explicitly and asserts a Sys verb exists for it (or
// documents the "有意缺格" it is intentionally NOT mapped to).
//
// Blind spot this test cannot close (spec's own申报): Recv/占用者弧/ctx
// provenance have no capability-face atom to enumerate against — those are
// verified by lib/actorbase's own engine tests (ledger/occupant-arc unit
// tests), not by this satisfies-the-face sweep. A surjection assertion is
// necessary, not sufficient — human review of the two files below remains
// the final authority for "did the atom enumeration itself stay complete".
func TestActorbaseSysIsSurjectiveOverCapabilityFace(t *testing.T) {
	sysType := reflect.TypeOf((*actorbase.Sys)(nil)).Elem()
	hasMethod := func(name string) bool {
		_, ok := sysType.MethodByName(name)
		return ok
	}

	// --- Pen atoms (runtime/harness/pen.go: Pen has exactly ONE method, Write;
	//     the semantic atoms are message.Kind × terminal status, not method
	//     count) ---------------------------------------------------------------
	penAtoms := map[string]string{
		"response/completed terminal":         "Reply",
		"response/failed terminal":            "Fail",
		"response/provisional (non-terminal)": "Progress",
		"event (no closure obligation)":       "Emit",
		"request + caller closure":            "Call",
	}
	for atom, verb := range penAtoms {
		if !hasMethod(verb) {
			t.Errorf("Pen atom %q has no Sys verb %s", atom, verb)
		}
	}
	var _ harness.Pen // named so a future Pen method addition is a visible diff to re-audit here.

	// --- Access/State atoms (runtime/accessdoor/handle.go: AccessHandle has
	//     exactly ONE method, Invoke; the semantic atoms are access.Operation
	//     values) -------------------------------------------------------------
	opAtoms := map[access.Operation]string{
		access.OpCreate: "Resource().Create / State().Put(create-fallthrough)",
		access.OpRead:   "Resource().Read / State().Get",
		access.OpWrite:  "Resource().Write / State().Put",
		access.OpDelete: "Resource().Delete / State().Del",
		// access.OpSet — 期11 spec §3's "词表糖名" landed ShareActor/
		// ShareMembers as its Sys verb (the PRE-期11 "no Sys method answers
		// it, by design" gap is closed): both are Invoke(OpSet) sugar over an
		// actor-kind / members-kind Grant respectively, so a Proc body never
		// hand-assembles one.
		access.OpSet: "Resource().ShareActor / Resource().ShareMembers",
	}
	if !hasMethod("Resource") || !hasMethod("State") {
		t.Fatal("Sys is missing the Resource()/State() arms entirely")
	}
	for op, mapped := range opAtoms {
		_ = op
		_ = mapped // documentation-only: ResourceHandle/StateHandle's own methods
		// (checked below) are the actual mechanical assertion; this map exists so
		// a reviewer sees the atom ↔ verb pairing in one place.
	}
	resourceType := reflect.TypeOf((*actorbase.ResourceHandle)(nil)).Elem()
	for _, m := range []string{"Create", "Read", "Write", "Delete", "ShareActor", "ShareMembers"} {
		if _, ok := resourceType.MethodByName(m); !ok {
			t.Errorf("ResourceHandle has no %s method for access.Operation atom", m)
		}
	}
	stateType := reflect.TypeOf((*actorbase.StateHandle)(nil)).Elem()
	for _, m := range []string{"Get", "Put", "Del"} {
		if _, ok := stateType.MethodByName(m); !ok {
			t.Errorf("StateHandle has no %s method for access.Operation atom", m)
		}
	}
	// 期11 spec §3.6/§3.7's Stat/List are Query methods, not Operation atoms
	// (deliberately NOT in opAtoms above — §3.8's "Stat/List 绝不进 Operation
	// 闭集"), but they are still part of the resource face's public surface
	// this surjection sweep documents.
	for _, m := range []string{"Stat", "List"} {
		if _, ok := resourceType.MethodByName(m); !ok {
			t.Errorf("ResourceHandle has no %s method (期11 §3 read face)", m)
		}
	}
	var _ accessdoor.AccessHandle         // same "single Invoke method" tripwire as Pen above.
	var _ accessdoor.ResourceAccessHandle // the resource face's own (Invoke+Create+Stat+List) tripwire.
	// access.OpSet completes the 5-member Operation set (Create/Read/Write/Set/
	// Delete) this manifest enumerates against; allOperations itself is
	// unexported (protocol/access/operation.go: "the closed-set contract is the
	// switch in ParseOperation"), so this file names OpSet directly rather than
	// re-deriving the count — a sixth Operation added there without a
	// corresponding line here is a review-layer gap, not a mechanical one.
	var _ = access.OpSet

	// --- Schedule arm ---------------------------------------------------------
	for _, m := range []string{"After", "CancelTimer"} {
		if !hasMethod(m) {
			t.Errorf("Schedule atom has no Sys verb %s", m)
		}
	}

	// --- Lifecycle arm ---------------------------------------------------------
	for _, m := range []string{"Fork", "End"} {
		if !hasMethod(m) {
			t.Errorf("Lifecycle atom has no Sys verb %s", m)
		}
	}

	// --- ActorContext atoms ------------------------------------------------------
	for _, m := range []string{"PublishObs", "Self"} {
		if !hasMethod(m) {
			t.Errorf("ActorContext atom has no Sys verb %s", m)
		}
	}

	// --- Input stream + process life -------------------------------------------
	for _, m := range []string{"Recv", "Life"} {
		if !hasMethod(m) {
			t.Errorf("input-stream/process-life atom has no Sys verb %s", m)
		}
	}
}
