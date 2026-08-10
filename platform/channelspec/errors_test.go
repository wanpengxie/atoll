package channelspec

import "testing"

func TestSpaceErrorCodeClosedSet(t *testing.T) {
	want := map[SpaceErrorCode]bool{
		SpaceForbidden: true, SpaceDeclNotFound: true, SpaceResourceNotFound: true,
		SpaceCapabilityUnavailable: true, SpaceChannelUnavailable: true,
		SpaceUnavailable: true, SpaceInvalidRequest: true, SpaceConflict: true,
	}
	if len(spaceErrorCodes) != len(want) {
		t.Fatalf("space error closed set has %d entries, want %d", len(spaceErrorCodes), len(want))
	}
	for _, code := range spaceErrorCodes {
		if !want[code] {
			t.Fatalf("unexpected space error code %q", code)
		}
		delete(want, code)
	}
	if len(want) != 0 {
		t.Fatalf("missing space error codes %v", want)
	}
}
