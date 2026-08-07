package home_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
)

// TestHomePublicSurface pins the terminal *home.Home method set. Home exports
// only its read-only View; every cross-package operational capability is issued
// by channelhost as a narrow Bundle facet. Any new Home method is therefore an
// organ-bag regression and turns this test red.
func TestHomePublicSurface(t *testing.T) {
	want := []string{"View"}

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
