package home_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
)

// TestHomePublicSurface pins *home.Home's exported method set to EXACTLY the
// capabilities the design fixes: View / Admit / Remove / ServeAttach /
// Subscribe / CancelRequest / KickDaemon / Close + the subjectgate slot seam.
// The gateway 期 removed the HumanHandle door (Human/HumanPrincipal): an
// off-process subject now drives its own cell through the subjectgate frame
// protocol, so no synchronous door face hangs off Home. Admit is the
// pure-membership primitive (neutral row + ring poke); embodiment is the
// ring's, never Admit's.
// Gate is GONE under sealed-pen —
// Home no longer hands out a bare write gate; it Mints a welded Pen internally at
// each admission point (the Minter never escapes). Remove (期8 S1) is the
// identity-level termination paved path — a composition over DespawnID +
// ApplyMemberTransitions, never a bare runtime/store accessor. KickDaemon
// (期8 S3) is the revocation paved path — a thin wrapper over the link
// Acceptor's per-compute handle table, never a bare Acceptor accessor. This is
// the mechanical guard against the organ-bag regression — any added accessor
// (re-exposing bare Runtime / Deliverer / Membership / Registry / Minter, or
// handing out an internal object instead of a capability method) turns this
// test red (assembly hands out keys only — compile-time red line).
func TestHomePublicSurface(t *testing.T) {
	// PresenceSweptCount joined with the W4 presence/obs axis — the corollary-two
	// enforcement read-out (sweep-cleared orphan tally, DoD-12): a read-only
	// counter, not an accessor to any internal organ.
	// EnsureSubjectSlot/SubjectSlotFor/RemoveSubjectSlot joined with the gateway
	// 期 S3: the per-identity binding slot seam the drivers/gateway伞包 drives
	// through (the concrete Registry stays internal — these hand out the exported
	// *subjectgate.Slot capability handle, never the bare registry object).
	// ResolvePrincipal/PrincipalOf are the principal↔actor-id resolution the
	// gateway + operate shim use to reach a subject's slot (no door handle).
	want := []string{"Admit", "ApplyRestartTarget", "CancelRequest", "Close", "Composition", "CompositionByPrincipal", "DefaultAgent", "EnsureSubjectSlot", "HasComposition", "IntroduceComposition", "KickDaemon", "MarkCompositionMigrated", "PresenceSweptCount", "PrincipalOf", "Remove", "RemoveInstance", "RemoveSubjectSlot", "ResolvePrincipal", "Restart", "RestartInstanceDirect", "RevokeDaemonTarget", "ServeAttach", "SetDefaultAgent", "SubjectSlotFor", "Subscribe", "View"}

	typ := reflect.TypeOf((*home.Home)(nil))
	var got []string
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("home.Home public method set = %v, want exactly %v (%d vs %d methods)",
			got, want, len(got), len(want))
	}
}
