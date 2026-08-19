package channelspec

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
