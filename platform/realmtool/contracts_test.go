package realmtool

import (
	"strings"
	"testing"
)

func TestDerivedRefsAreDomainSeparatedAndChannelScoped(t *testing.T) {
	a := DerivedRealmToolRef("a", "same")
	b := DerivedRealmToolRef("b", "same")
	if a == b {
		t.Fatal("same request id collided across channels")
	}
	if !strings.HasPrefix(a, "adm:rt:v1:") {
		t.Fatal("reference family/version prefix missing")
	}
}

// The realm error code closed-set test moved to platform/channelspec with the
// vocabulary itself.
