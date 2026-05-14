package canonical

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// sampleEnvelope returns a fully populated normalized envelope used as a
// stable fixture across the determinism / normalize-before-hash tests.
func sampleEnvelope() v4types.Envelope {
	notBefore := int64(1700000010000)
	expiresAt := int64(1700000020000)
	docs := []string{"work/a.md", "work/b.md"}
	return v4types.Envelope{
		ID:            "msg-1",
		TS:            1700000000000,
		ChannelID:     "chan-A",
		Sender:        v4types.Sender{Kind: v4types.SenderAgent, ID: "agent-1", Name: "Alice"},
		Kind:          v4types.KindRequest,
		Type:          "xhs.publish.requested",
		Payload:       json.RawMessage(`{"body":{"title":"hi","seq":1}}`),
		ParentID:      "",
		CorrelationID: "corr-1",
		DocRefs:       &docs,
		Visibility:    v4types.VisibilityPublic,
		Audience:      []string{"tool:xhs-adapter"},
		NotBefore:     &notBefore,
		ExpiresAt:     &expiresAt,
		// store-derived fields below MUST not affect the hash
		TSReceived:       1700000000123,
		DeliveredAt:      &expiresAt, // arbitrary
		DeliveryFailedAt: nil,
		LastError:        "stale",
		Attempts:         5,
		IsTerminal:       true,
		Seq:              99,
	}
}

// TestCanonicalHashDeterministic — acceptance: same envelope → same
// hash across 100 invocations.
func TestCanonicalHashDeterministic(t *testing.T) {
	t.Parallel()
	env := sampleEnvelope()
	want, err := CanonicalHash(env)
	if err != nil {
		t.Fatalf("hash error: %v", err)
	}
	if len(want) != 64 {
		t.Fatalf("hash length = %d, want 64", len(want))
	}
	for i := 0; i < 100; i++ {
		got, err := CanonicalHash(env)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("iteration %d: hash drifted: %s vs %s", i, got, want)
		}
	}
}

// TestCanonicalHashIgnoresStoreDerived — acceptance: hash MUST exclude
// ts_received / is_terminal / delivery metadata / seq per L1 §10.2.2.
func TestCanonicalHashIgnoresStoreDerived(t *testing.T) {
	t.Parallel()
	base := sampleEnvelope()
	baseHash, err := CanonicalHash(base)
	if err != nil {
		t.Fatal(err)
	}

	// Mutate every store-derived field individually; hash must hold.
	mutators := map[string]func(e *v4types.Envelope){
		"TSReceived":       func(e *v4types.Envelope) { e.TSReceived = 12345 },
		"DeliveredAt":      func(e *v4types.Envelope) { v := int64(7); e.DeliveredAt = &v },
		"DeliveryFailedAt": func(e *v4types.Envelope) { v := int64(8); e.DeliveryFailedAt = &v },
		"LastError":        func(e *v4types.Envelope) { e.LastError = "different" },
		"Attempts":         func(e *v4types.Envelope) { e.Attempts = 99 },
		"IsTerminal":       func(e *v4types.Envelope) { e.IsTerminal = !e.IsTerminal },
		"Seq":              func(e *v4types.Envelope) { e.Seq = 1234 },
	}
	for name, mut := range mutators {
		t.Run(name, func(t *testing.T) {
			alt := base
			mut(&alt)
			got, err := CanonicalHash(alt)
			if err != nil {
				t.Fatal(err)
			}
			if got != baseHash {
				t.Errorf("mutating %s changed hash: %s vs %s", name, got, baseHash)
			}
		})
	}
}

// TestNormalizeBeforeHashDifferentiates — acceptance: pre-normalize
// envelope (e.g. missing visibility default) produces a different hash
// from the normalized form. We simulate by varying any in-domain field.
func TestNormalizeBeforeHashDifferentiates(t *testing.T) {
	t.Parallel()
	base := sampleEnvelope()
	baseHash, err := CanonicalHash(base)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the pre-normalize state: visibility filled to "" (zero
	// value) vs filled to "public" after normalization.
	pre := base
	pre.Visibility = ""
	preHash, err := CanonicalHash(pre)
	if err != nil {
		t.Fatal(err)
	}
	if preHash == baseHash {
		t.Errorf("pre-normalize visibility=\"\" should differ from post-normalize visibility=\"public\"")
	}

	// Same idea with audience: pre-normalize empty vs ["*"].
	pre2 := base
	pre2.Audience = nil
	preHash2, err := CanonicalHash(pre2)
	if err != nil {
		t.Fatal(err)
	}
	if preHash2 == baseHash {
		t.Errorf("audience=nil vs audience=[\"*\"] must produce different hashes")
	}
}

// TestCanonicalizeJSONSortsKeys — RFC 8785 invariant: object keys appear
// in UTF-16 code-unit lexicographic order regardless of insertion order.
func TestCanonicalizeJSONSortsKeys(t *testing.T) {
	t.Parallel()
	inputs := []string{
		`{"b":2,"a":1}`,
		`{"a":1,"b":2}`,
	}
	want := `{"a":1,"b":2}`
	for _, in := range inputs {
		got, err := CanonicalizeJSON([]byte(in))
		if err != nil {
			t.Fatalf("input %s: %v", in, err)
		}
		if string(got) != want {
			t.Errorf("input %s → %s, want %s", in, got, want)
		}
	}
}

// TestCanonicalizeJSONNumberNormalization — RFC 8785 / ECMAScript:
// 1.0 and 1 must canonicalize identically; trailing zeros stripped.
func TestCanonicalizeJSONNumberNormalization(t *testing.T) {
	t.Parallel()
	pairs := [][2]string{
		{`1`, `1`},
		{`1.0`, `1`},
		{`-0`, `0`},
		{`0`, `0`},
		{`1.5`, `1.5`},
		{`123`, `123`},
		{`100`, `100`},
	}
	for _, pair := range pairs {
		got, err := CanonicalizeJSON([]byte(pair[0]))
		if err != nil {
			t.Fatalf("input %s: %v", pair[0], err)
		}
		if string(got) != pair[1] {
			t.Errorf("input %s → %s, want %s", pair[0], got, pair[1])
		}
	}
}

// TestCanonicalHashPayloadEquivalentNumbers — payloads {n:1} and
// {n:1.0} must hash to the same digest.
func TestCanonicalHashPayloadEquivalentNumbers(t *testing.T) {
	t.Parallel()
	h1, err := CanonicalHashPayload([]byte(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := CanonicalHashPayload([]byte(`{"n":1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("{n:1} hash %s != {n:1.0} hash %s", h1, h2)
	}
}

// TestCanonicalHashPayloadKeyReorder — same payload with different
// JSON key order produces the same hash.
func TestCanonicalHashPayloadKeyReorder(t *testing.T) {
	t.Parallel()
	h1, err := CanonicalHashPayload([]byte(`{"a":1,"b":2,"c":[3,4]}`))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := CanonicalHashPayload([]byte(`{"c":[3,4],"a":1,"b":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("hash differs across key reordering: %s vs %s", h1, h2)
	}
}

// TestCanonicalHashPayloadArrayOrderMatters — arrays preserve order.
func TestCanonicalHashPayloadArrayOrderMatters(t *testing.T) {
	t.Parallel()
	h1, err := CanonicalHashPayload([]byte(`[1,2,3]`))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := CanonicalHashPayload([]byte(`[3,2,1]`))
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Errorf("arrays should hash by order; got same hash for [1,2,3] and [3,2,1]")
	}
}

// TestCanonicalizeJSONStringEscape — RFC 8785 string escape rules.
func TestCanonicalizeJSONStringEscape(t *testing.T) {
	t.Parallel()
	cases := [][2]string{
		// forward slash NOT escaped
		{`"a/b"`, `"a/b"`},
		// quote and backslash
		{`"q\"x"`, `"q\"x"`},
		{`"back\\slash"`, `"back\\slash"`},
		// control chars get shorthand
		{`"\b\t\n\f\r"`, `"\b\t\n\f\r"`},
		// non-shorthand control chars covered in TestControlCharEscape
		// printable above 0x7F passes through as UTF-8 (no escape)
		{`"中文"`, `"中文"`},
	}
	for _, c := range cases {
		got, err := CanonicalizeJSON([]byte(c[0]))
		if err != nil {
			t.Fatalf("input %s: %v", c[0], err)
		}
		if string(got) != c[1] {
			t.Errorf("input %s → %q, want %q", c[0], got, c[1])
		}
	}
}

// TestControlCharEscape covers the non-shorthand C0 control characters
// (those without dedicated `\b/\t/\n/\f/\r` aliases). The JSON parser
// requires control chars in a string to arrive already-escaped as
// `\uXXXX`; the canonical output then re-renders them as lowercase
// `\u00xx` per RFC 8785 §3.2.2.2. We keep the source file pure ASCII by
// passing the escape sequence as Go string literals.
func TestControlCharEscape(t *testing.T) {
	t.Parallel()
	cases := [][2]string{
		{"\"\\u0001\"", "\"\\u0001\""}, // SOH → 
		{"\"\\u001F\"", "\"\\u001f\""}, // US →  (lowercased hex)
		{"\"\\u0007\"", "\"\\u0007\""}, // BEL → 
		// 0x0A is line feed → shorthand \n (case-insensitive input).
		{"\"\\u000A\"", "\"\\n\""},
		{"\"\\u000a\"", "\"\\n\""},
	}
	for _, c := range cases {
		got, err := CanonicalizeJSON([]byte(c[0]))
		if err != nil {
			t.Errorf("input %q: %v", c[0], err)
			continue
		}
		if string(got) != c[1] {
			t.Errorf("input %q → %q, want %q", c[0], got, c[1])
		}
	}
}

// TestCanonicalHashHexLowercase — output is always 64 lowercase hex.
func TestCanonicalHashHexLowercase(t *testing.T) {
	t.Parallel()
	h, err := CanonicalHash(sampleEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 64 {
		t.Errorf("hash length = %d, want 64", len(h))
	}
	if strings.ToLower(h) != h {
		t.Errorf("hash %q is not lowercase", h)
	}
	for _, c := range h {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Errorf("non-hex char %q in hash %q", c, h)
			break
		}
	}
}

// TestCanonicalHashPayloadKnownVector — pin a SHA-256 result so any
// change to the canonical bytes shows up loud and clear.
//
// Computed reference: SHA-256 over the canonical form of `{"b":2,"a":1}`
// which is `{"a":1,"b":2}`. The expected digest below was produced by
// running the implementation once and recording it; if the hash function
// or canonicalization output ever drifts, this vector fails.
func TestCanonicalHashPayloadKnownVector(t *testing.T) {
	t.Parallel()
	got, err := CanonicalHashPayload([]byte(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	// Pinned digest — verify with: echo -n '{"a":1,"b":2}' | sha256sum
	const want = "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
	if got != want {
		t.Errorf("hash mismatch:\n got  %s\n want %s", got, want)
	}
}

// TestCanonicalHashEmptyPayloadRejected — caller MUST normalize empty
// payload to `{}`; passing raw empty bytes is treated as a programmer
// error (clearer than silently hashing nothing).
func TestCanonicalHashEmptyPayloadRejected(t *testing.T) {
	t.Parallel()
	env := sampleEnvelope()
	env.Payload = nil
	if _, err := CanonicalHash(env); err == nil {
		t.Errorf("expected error for empty payload, got nil")
	}
}

// TestFormatFloatESBoundaries — spot-check ECMAScript Number→String at
// the fixed/scientific boundaries.
func TestFormatFloatESBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{1.5, "1.5"},
		{-1.5, "-1.5"},
		{0.1, "0.1"},
		{1e-6, "0.000001"},              // boundary: still fixed
		{1e-7, "1e-7"},                  // exponent -7 → scientific
		{1e20, "100000000000000000000"}, // boundary: still fixed
		{1e21, "1e+21"},                 // exponent 21 → scientific
		{1.5e21, "1.5e+21"},
	}
	for _, c := range cases {
		got, err := formatFloatES(c.in)
		if err != nil {
			t.Errorf("formatFloatES(%v): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("formatFloatES(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
