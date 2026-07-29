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

func TestRealmErrorCodeClosedSet(t *testing.T) {
	want := map[RealmErrorCode]bool{
		RealmForbidden: true, RealmDeclNotFound: true, RealmResourceNotFound: true,
		RealmCapabilityUnavailable: true, RealmChannelUnavailable: true,
		RealmUnavailable: true, RealmInvalidRequest: true, RealmConflict: true,
	}
	if len(realmErrorCodes) != len(want) {
		t.Fatalf("realm error closed set has %d entries, want %d", len(realmErrorCodes), len(want))
	}
	for _, code := range realmErrorCodes {
		if !want[code] {
			t.Fatalf("unexpected realm error code %q", code)
		}
		delete(want, code)
	}
	if len(want) != 0 {
		t.Fatalf("missing realm error codes %v", want)
	}
}
