package platform_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/ActOS/platform"
)

// TestHomePublicSurface pins *platform.Home's exported method set to EXACTLY the
// six capabilities the design fixes (§6 of platform-redesign-construction):
// Gate / View / Spawn / Links / Taps / Close. This is the机械守卫 against the
// organ-bag regression — any added accessor (re-exposing裸 Runtime / Deliverer /
// Membership / Registry, etc.) turns this test red (装配只交钥匙红线).
func TestHomePublicSurface(t *testing.T) {
	want := []string{"Close", "Gate", "Links", "Spawn", "Taps", "View"}

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
