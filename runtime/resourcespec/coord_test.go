package resourcespec

import "testing"

// TestGenerateCoordShapeAndUniqueness pins §1.6/design doc C1's contract: a
// fixed-width hex string (sha256 digest), never empty, never repeating
// across calls (random seed) — the two properties a placement_coord value
// must have to be safe as an opaque storage handle.
func TestGenerateCoordShapeAndUniqueness(t *testing.T) {
	const wantLen = 64 // hex(sha256) = 32 bytes * 2
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		coord, err := GenerateCoord()
		if err != nil {
			t.Fatalf("GenerateCoord() error = %v", err)
		}
		if len(coord) != wantLen {
			t.Fatalf("GenerateCoord() len = %d, want %d (coord=%q)", len(coord), wantLen, coord)
		}
		if seen[coord] {
			t.Fatalf("GenerateCoord() produced a repeat: %q", coord)
		}
		seen[coord] = true
	}
}
