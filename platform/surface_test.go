package platform_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/atoll/platform"
)

// TestHomePublicSurface pins *platform.Home's exported method set to EXACTLY the
// nine capabilities the design fixes: View / Admit / Spawn / Remove / ServeAttach /
// Subscribe / CancelRequest / KickDaemon / Close. Admit is the pure-membership
// primitive (neutral row + ring poke); embodiment is the ring's, never Admit's.
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
	want := []string{"Admit", "CancelRequest", "Close", "KickDaemon", "Remove", "ServeAttach", "Spawn", "Subscribe", "View"}

	typ := reflect.TypeOf((*platform.Home)(nil))
	var got []string
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("platform.Home public method set = %v, want exactly %v (%d vs %d methods)",
			got, want, len(got), len(want))
	}
}
