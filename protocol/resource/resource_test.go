package resource_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/protocol/resource"
)

// resource.ResourceID is an opaque stable-string newtype — the passive object
// of the access plane. The object has no gift, hence no incarnation: its
// coordinate is single-level (this id + current state). The kernel treats the
// id as opaque: no normalization, no validation, no allocation policy. These
// tests pin that opacity contract (the substrate must not silently mangle an
// addressing token) and the value semantics the access relation relies on.

// TestResourceIDStringIsExactIdentity — String() returns the underlying wire
// form byte-for-byte. No trimming, case-folding, or normalization is permitted:
// the kernel is opaque, so any string survives the round trip verbatim.
func TestResourceIDStringIsExactIdentity(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",                // zero value
		"res-1",           // ascii
		"resource:abc:0",  // colons (no special meaning to the kernel)
		"res:文字",          // multibyte utf-8
		" leading-space",  // surrounding whitespace must NOT be trimmed
		"trailing-space ", //
		"  ",              // whitespace-only
		"UPPER",           // case must NOT be folded
		"a/b/c",           // slashes carry no meaning
		"\t\n",            // control chars pass through
		"emoji-😀",         // astral plane
	}
	for _, c := range cases {
		if got := resource.ResourceID(c).String(); got != c {
			t.Errorf("resource.ResourceID(%q).String() = %q, want exact identity %q", c, got, c)
		}
	}
}

// TestResourceIDNewtypeRoundTrip — ResourceID is a distinct type over string yet
// losslessly convertible both directions. Conversion is pure relabeling, never
// mutation.
func TestResourceIDNewtypeRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []string{"", "res-1", "res:文字", "  spaced  "}
	for _, s := range cases {
		if back := string(resource.ResourceID(s)); back != s {
			t.Errorf("string(resource.ResourceID(%q)) = %q, want %q", s, back, s)
		}
	}
}

// TestResourceIDStringMatchesConversion — String() and an explicit string()
// conversion agree. String() must be a plain accessor, not a transform.
func TestResourceIDStringMatchesConversion(t *testing.T) {
	t.Parallel()
	cases := []string{"", "res-1", "resource:abc:0", "res:文字", " x "}
	for _, s := range cases {
		id := resource.ResourceID(s)
		if id.String() != string(id) {
			t.Errorf("ResourceID(%q): String()=%q != string()=%q", s, id.String(), string(id))
		}
	}
}

// TestResourceIDValueEquality — ResourceIDs are comparable by value. The access
// relation keys objects by ResourceID; equality must be exact-string equality
// (no normalization collapsing distinct tokens, no distinctness between equal
// tokens).
func TestResourceIDValueEquality(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want bool
	}{
		{"res-1", "res-1", true},
		{"", "", true},
		{"res:文字", "res:文字", true},
		{"res-1", "res-2", false},
		{"res-1", "Res-1", false},  // case-sensitive
		{"res-1", "res-1 ", false}, // whitespace-sensitive
		{"", " ", false},           // empty != whitespace
		{"a/b", "a%2Fb", false},    // no escaping equivalence
	}
	for _, c := range cases {
		if got := resource.ResourceID(c.a) == resource.ResourceID(c.b); got != c.want {
			t.Errorf("resource.ResourceID(%q) == resource.ResourceID(%q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestResourceIDUsableAsMapKey — the access subsystem keys objects by
// ResourceID, so it must work as a comparable map key with exact-token
// semantics.
func TestResourceIDUsableAsMapKey(t *testing.T) {
	t.Parallel()
	m := map[resource.ResourceID]int{}
	m[resource.ResourceID("res-1")] = 1
	m[resource.ResourceID("res-2")] = 2
	m[resource.ResourceID("res-1")] = 3 // overwrites same token

	if got := m[resource.ResourceID("res-1")]; got != 3 {
		t.Errorf("m[res-1] = %d, want 3 (overwrite of equal token)", got)
	}
	if got := m[resource.ResourceID("res-2")]; got != 2 {
		t.Errorf("m[res-2] = %d, want 2", got)
	}
	if _, ok := m[resource.ResourceID("Res-1")]; ok {
		t.Error("m[Res-1] present, want absent (key is case-sensitive)")
	}
	if len(m) != 2 {
		t.Errorf("len(m) = %d, want 2", len(m))
	}
}

// TestResourceIDZeroValueIsEmptyOpaque — the zero ResourceID is the empty string
// and is a fully representable value. The opaque kernel rejects nothing;
// emptiness is a domain concern, not a kernel-enforced invariant.
func TestResourceIDZeroValueIsEmptyOpaque(t *testing.T) {
	t.Parallel()
	var zero resource.ResourceID
	if zero != "" {
		t.Errorf("zero resource.ResourceID = %q, want empty string", zero)
	}
	if zero.String() != "" {
		t.Errorf("zero.String() = %q, want empty string", zero.String())
	}
}
