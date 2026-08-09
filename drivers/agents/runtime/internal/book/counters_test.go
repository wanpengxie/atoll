package book

import "testing"

func TestCountersAreIndependentNonZeroSequences(t *testing.T) {
	var c Counters
	if got, ok := c.Generation(); !ok || got != 1 {
		t.Fatalf("generation=%d ok=%v", got, ok)
	}
	if got, ok := c.Attempt(); !ok || got != 1 {
		t.Fatalf("attempt=%d ok=%v", got, ok)
	}
	if got, ok := c.Action(); !ok || got != 1 {
		t.Fatalf("action=%d ok=%v", got, ok)
	}
	if got, ok := c.Revision(); !ok || got != 1 {
		t.Fatalf("revision=%d ok=%v", got, ok)
	}
}
