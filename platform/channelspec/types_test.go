package channelspec

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestRenderedSnapshotDigestIsCanonicalContentIdentity(t *testing.T) {
	one, err := (RenderedSnapshot{
		Class: "agent", Config: json.RawMessage(`{"x":1}`),
		Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	two := one
	two.Config = json.RawMessage(` { "x" : 1.0 } `)
	got, err := two.ContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if got != one.Digest {
		t.Fatalf("canonical-equivalent content changed digest: %s != %s", got, one.Digest)
	}
}

func TestOperationErrorClosedSet(t *testing.T) {
	want := map[OperationErrorCode]bool{
		ErrCodeBadPayload: true, ErrCodeChannelUnavailable: true, ErrCodeInvalidDesiredHost: true,
		ErrCodeDeclNotFound: true, ErrCodeForbidden: true,
		ErrCodeUnknownClass: true, ErrCodeProtectedActor: true,
		ErrCodeNotAcceptedSource: true, ErrCodeMemberInactive: true,
		ErrCodeAuthorityUnavailable: true,
	}
	if len(operationErrorCodes) != len(want) {
		t.Fatalf("operate error closed set has %d entries, want %d", len(operationErrorCodes), len(want))
	}
	for _, code := range operationErrorCodes {
		if !want[code] {
			t.Fatalf("unexpected operate error code %q", code)
		}
		delete(want, code)
	}
	if len(want) != 0 {
		t.Fatalf("missing operate error codes %v", want)
	}
	for _, code := range operationErrorCodes {
		if string(code) == "unauthorized_sender" {
			t.Fatal("unauthorized_sender is a transport-gate refusal, not an operate error code")
		}
	}
}
