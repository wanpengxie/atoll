package channel_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// channel.ID is an opaque stable-string newtype. The kernel treats it as
// opaque: no normalization, no validation, no allocation policy. These tests
// pin that opacity contract (the substrate must not silently mangle an
// addressing token) and the value semantics A1 addressing relies on.

// TestIDStringIsExactIdentity — String() returns the underlying wire form
// byte-for-byte. No trimming, case-folding, or normalization is permitted:
// the kernel is opaque, so any string survives the round trip verbatim.
func TestIDStringIsExactIdentity(t *testing.T) {
	cases := []string{
		"",                // zero value
		"ch-1",            // ascii
		"channel:abc:0",   // colons (no special meaning to the kernel)
		"ch:文字",           // multibyte utf-8
		" leading-space",  // surrounding whitespace must NOT be trimmed
		"trailing-space ", //
		"  ",              // whitespace-only
		"UPPER",           // case must NOT be folded
		"a/b/c",           // slashes carry no meaning
		"\t\n",            // control chars pass through
		"emoji-😀",         // astral plane
	}
	for _, c := range cases {
		if got := channel.ID(c).String(); got != c {
			t.Errorf("channel.ID(%q).String() = %q, want exact identity %q", c, got, c)
		}
	}
}

// TestIDNewtypeRoundTrip — ID is a distinct type over string yet losslessly
// convertible both directions. Conversion is pure relabeling, never mutation.
func TestIDNewtypeRoundTrip(t *testing.T) {
	cases := []string{"", "ch-1", "ch:文字", "  spaced  "}
	for _, s := range cases {
		if back := string(channel.ID(s)); back != s {
			t.Errorf("string(channel.ID(%q)) = %q, want %q", s, back, s)
		}
	}
}

// TestIDStringMatchesConversion — String() and an explicit string() conversion
// agree. String() must be a plain accessor, not a transform.
func TestIDStringMatchesConversion(t *testing.T) {
	cases := []string{"", "ch-1", "channel:abc:0", "ch:文字", " x "}
	for _, s := range cases {
		id := channel.ID(s)
		if id.String() != string(id) {
			t.Errorf("ID(%q): String()=%q != string()=%q", s, id.String(), string(id))
		}
	}
}

// TestIDValueEquality — IDs are comparable by value. A1 addressing routes by
// ActorID/channel; equality must be exact-string equality (no normalization
// collapsing distinct tokens, no distinctness between equal tokens).
func TestIDValueEquality(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"ch-1", "ch-1", true},
		{"", "", true},
		{"ch:文字", "ch:文字", true},
		{"ch-1", "ch-2", false},
		{"ch-1", "Ch-1", false},  // case-sensitive
		{"ch-1", "ch-1 ", false}, // whitespace-sensitive
		{"", " ", false},         // empty != whitespace
		{"a/b", "a%2Fb", false},  // no escaping equivalence
	}
	for _, c := range cases {
		if got := channel.ID(c.a) == channel.ID(c.b); got != c.want {
			t.Errorf("channel.ID(%q) == channel.ID(%q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestIDUsableAsMapKey — substrate uses channel.ID as an addressing key, so it
// must work as a comparable map key with exact-token semantics.
func TestIDUsableAsMapKey(t *testing.T) {
	m := map[channel.ID]int{}
	m[channel.ID("ch-1")] = 1
	m[channel.ID("ch-2")] = 2
	m[channel.ID("ch-1")] = 3 // overwrites same token

	if got := m[channel.ID("ch-1")]; got != 3 {
		t.Errorf("m[ch-1] = %d, want 3 (overwrite of equal token)", got)
	}
	if got := m[channel.ID("ch-2")]; got != 2 {
		t.Errorf("m[ch-2] = %d, want 2", got)
	}
	if _, ok := m[channel.ID("Ch-1")]; ok {
		t.Error("m[Ch-1] present, want absent (key is case-sensitive)")
	}
	if len(m) != 2 {
		t.Errorf("len(m) = %d, want 2", len(m))
	}
}

// TestIDZeroValueIsEmptyOpaque — the zero ID is the empty string and is a
// fully representable value. The opaque kernel rejects nothing; emptiness is
// a domain concern, not a kernel-enforced invariant.
func TestIDZeroValueIsEmptyOpaque(t *testing.T) {
	var zero channel.ID
	if zero != "" {
		t.Errorf("zero channel.ID = %q, want empty string", zero)
	}
	if zero.String() != "" {
		t.Errorf("zero.String() = %q, want empty string", zero.String())
	}
}
