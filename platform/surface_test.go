package platform_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/atoll/platform"
)

// TestHomePublicSurface pins *platform.Home's exported method set to EXACTLY the
// five capabilities the design fixes: View / Spawn / ServeAttach / Subscribe /
// Close. Gate is GONE under sealed-pen — Home no longer hands out a bare write gate;
// it Mints a welded Pen internally at each admission point (the Minter never
// escapes). This is the mechanical guard against the organ-bag regression — any added
// accessor (re-exposing bare Runtime / Deliverer / Membership / Registry / Minter,
// or handing out an internal object instead of a capability method) turns this
// test red (assembly hands out keys only — compile-time red line).
func TestHomePublicSurface(t *testing.T) {
	want := []string{"Close", "ServeAttach", "Spawn", "Subscribe", "View"}

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
