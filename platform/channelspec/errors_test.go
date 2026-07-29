package channelspec

import "testing"

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
