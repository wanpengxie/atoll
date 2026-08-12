package lagoon

import (
	"strings"
	"testing"
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
