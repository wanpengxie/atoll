package channel

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalJSONAndDigest(t *testing.T) {
	got, err := CanonicalJSON(json.RawMessage(`{"z":1.0,"a":"<ok>","nested":{"b":2,"a":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"a":"<ok>","nested":{"a":1,"b":2},"z":1}`
	if string(got) != want {
		t.Fatalf("CanonicalJSON=%s want %s", got, want)
	}
	d1, _ := Digest(json.RawMessage(`{"b":2,"a":1}`))
	d2, _ := Digest(json.RawMessage(` { "a" : 1.0, "b" : 2 } `))
	if d1 != d2 || !strings.HasPrefix(d1, "v1:") {
		t.Fatalf("digest mismatch: %q %q", d1, d2)
	}
}

func TestRenderedSnapshotDigestExcludesSequence(t *testing.T) {
	one, err := (RenderedSnapshot{Class: "agent", Config: json.RawMessage(`{"x":1}`), Placement: Placement{Kind: PlacementServer}, RenderSeq: 1}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	two := one
	two.RenderSeq = 2
	got, err := two.ContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if got != one.Digest {
		t.Fatalf("sequence changed content digest: %s != %s", got, one.Digest)
	}
}

func TestDerivedRefsAreDomainSeparatedAndChannelScoped(t *testing.T) {
	a := DerivedRealmToolRef("a", "same")
	b := DerivedRealmToolRef("b", "same")
	if a == b {
		t.Fatal("same request id collided across channels")
	}
	if !strings.HasPrefix(a, "adm:rt:v1:") || !strings.HasPrefix(DerivedFanoutRef("base", "a"), "fo:v1:") || !strings.HasPrefix(DerivedFinalizeRef("op", "decl"), "ifin:v1:") {
		t.Fatal("reference family/version prefix missing")
	}
}

func TestOperationErrorClosedSet(t *testing.T) {
	if len(AllOperationErrorCodes) != 15 {
		t.Fatalf("operate error closed set has %d entries", len(AllOperationErrorCodes))
	}
}
