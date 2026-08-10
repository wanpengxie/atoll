package spacetool

import (
	"strings"
	"testing"
)

func TestDerivedRefsAreDomainSeparatedAndChannelScoped(t *testing.T) {
	a := DerivedSpaceToolRef("a", "same")
	b := DerivedSpaceToolRef("b", "same")
	if a == b {
		t.Fatal("same request id collided across channels")
	}
	if !strings.HasPrefix(a, "adm:st:v1:") {
		t.Fatal("reference family/version prefix missing")
	}
}

// The space error code closed-set test moved to platform/channelspec with the
// vocabulary itself.
