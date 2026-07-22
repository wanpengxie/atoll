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

func TestCanonicalJSONRFC8785Vectors(t *testing.T) {
	input := json.RawMessage(`{"numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],"string":"€$\u000f\nA'B\"\\\\\"/"}`)
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}`
	if string(got) != want {
		t.Fatalf("RFC 8785 vector:\n got %s\nwant %s", got, want)
	}

	// Property names sort as UTF-16 code units, and U+2028 is emitted literally.
	got, err = CanonicalJSON(map[string]any{"\ue000": 1, "😀": "\u2028"})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "{\"😀\":\"\u2028\",\"\":1}" {
		t.Fatalf("UTF-16/string canonicalization=%q", got)
	}
}

func TestRenderedSnapshotDigestIsCanonicalContentIdentity(t *testing.T) {
	one, err := (RenderedSnapshot{Class: "agent", Config: json.RawMessage(`{"x":1}`), Placement: Placement{Kind: PlacementServer}}).Seal()
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

func TestOperationCorrelationDomainsCannotCollide(t *testing.T) {
	ref := RefCorrelation("same-client-value")
	msg := MessageCorrelation("same-client-value")
	if ref == msg || !strings.HasPrefix(ref, "op:ref:v1:") || !strings.HasPrefix(msg, "op:msg:v1:") {
		t.Fatalf("correlations are not domain separated: ref=%q msg=%q", ref, msg)
	}
}

func TestOperationErrorClosedSet(t *testing.T) {
	want := map[OperationErrorCode]bool{
		ErrCodeBadPayload: true, ErrCodeChannelUnavailable: true, ErrCodeInvalidDesiredHost: true,
		ErrCodeDeclNotFound: true, ErrCodeForbidden: true,
		ErrCodeUnknownClass: true, ErrCodeProtectedActor: true, ErrCodeNotInComposition: true,
		ErrCodeInternal:          true,
		ErrCodeNotAcceptedSource: true, ErrCodeMemberInactive: true, ErrCodeAuthorityUnavailable: true,
		ErrCodeUnauthorizedSender: true,
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
	// ref_conflict is an A2 contract code (OpEntry typed error), NOT an operate
	// frame code — spec pins it out of this closed set. Nail that decision down.
	for _, code := range operationErrorCodes {
		if code == ErrCodeRefConflict {
			t.Fatal("ref_conflict must not be in the operate error closed set")
		}
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
