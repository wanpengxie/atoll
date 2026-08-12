package lagoon

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func TestValidateDeviceNameBoundaries(t *testing.T) {
	valid := []string{"a", strings.Repeat("a", 63), "a-1"}
	for _, name := range valid {
		if err := ValidateDeviceName(name); err != nil {
			t.Errorf("valid name %q: %v", name, err)
		}
	}
	invalid := []string{"", strings.Repeat("a", 64), "-a", "a-", "A", "a.b", "a_b", "a b"}
	for _, name := range invalid {
		if err := ValidateDeviceName(name); err == nil {
			t.Errorf("invalid name %q accepted", name)
		}
	}
}

func TestAddressLayerRoundTripsNamesThatMintingRejects(t *testing.T) {
	for _, name := range []string{"UPPER", "a.b"} {
		raw := "daemon://" + name + "/x"
		addr, err := resourcespec.ParseFileAddress(raw)
		if err != nil {
			t.Fatalf("address layer rejected %q: %v", raw, err)
		}
		got, err := resourcespec.FormatFileAddress(addr)
		if err != nil || got != raw {
			t.Fatalf("address round trip = %q, %v; want %q", got, err, raw)
		}
		if err := ValidateDeviceName(name); err == nil {
			t.Fatalf("minting layer accepted invalid machine name %q", name)
		}
	}
}
