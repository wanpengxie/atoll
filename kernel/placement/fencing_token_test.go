package placement_test

import (
	"encoding/hex"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/placement"
)

// TestNewFencingToken_HexFormat asserts the token shape contract per
// proto-foundation §3.6.1: 32-char lowercase hex (16 random bytes).
func TestNewFencingToken_HexFormat(t *testing.T) {
	tok, err := placement.NewFencingToken()
	if err != nil {
		t.Fatalf("NewFencingToken: %v", err)
	}
	s := string(tok)
	if len(s) != 32 {
		t.Errorf("len=%d want 32 (hex of 16 bytes); got %q", len(s), s)
	}
	if _, err := hex.DecodeString(s); err != nil {
		t.Errorf("token not valid hex: %v (token=%q)", err, s)
	}
	// lowercase contract — encoding/hex emits lowercase.
	for i, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		t.Errorf("non-lowercase-hex rune %q at index %d in %q", r, i, s)
		break
	}
}

// TestNewFencingToken_Unique asserts two consecutive calls return
// different tokens — the crypto/rand source is what makes fencing
// unguessable.
func TestNewFencingToken_Unique(t *testing.T) {
	const n = 64
	seen := make(map[placement.FencingToken]struct{}, n)
	for i := 0; i < n; i++ {
		tok, err := placement.NewFencingToken()
		if err != nil {
			t.Fatalf("NewFencingToken[%d]: %v", i, err)
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate token %q at iteration %d", tok, i)
		}
		seen[tok] = struct{}{}
	}
}

// TestFencingToken_OwnerEpochDecoupled asserts FencingToken is NOT
// derived from OwnerEpoch. The implementer's previous error was to
// equate them; spec (proto-foundation §3.6.1 + proto-layer1 §6.2)
// explicitly decouples the two.
//
// Concretely: generating two tokens at the same conceptual epoch must
// still yield different opaque values (the algorithm has no epoch input).
func TestFencingToken_OwnerEpochDecoupled(t *testing.T) {
	t1, err := placement.NewFencingToken()
	if err != nil {
		t.Fatal(err)
	}
	t2, err := placement.NewFencingToken()
	if err != nil {
		t.Fatal(err)
	}
	if t1 == t2 {
		t.Errorf("two tokens at same epoch are identical (%q) — algorithm leaks epoch", t1)
	}
	// Sanity: an OwnerEpoch value cast to int64 cannot equal a hex token.
	// Use a concrete sample.
	epoch := placement.OwnerEpoch(1)
	if string(t1) == "1" || placement.FencingToken("1") == t1 {
		t.Errorf("token must not be the int64 stringification of epoch (epoch=%d, token=%q)", epoch, t1)
	}
}
