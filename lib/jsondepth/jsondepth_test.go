package jsondepth

import (
	"strings"
	"testing"
)

// TestBounded pins the depth gate: an over-deep blob is refused, a shallow one
// passes, and a malformed blob is left for the caller's own decode (nil here).
func TestBounded(t *testing.T) {
	over := strings.Repeat("[", MaxDepth+1) + strings.Repeat("]", MaxDepth+1)
	if err := Bounded([]byte(over)); err == nil {
		t.Fatal("Bounded accepted an over-deep blob")
	}
	if err := Bounded([]byte(`{"a":{"b":1}}`)); err != nil {
		t.Fatalf("Bounded rejected a shallow blob: %v", err)
	}
	// Exactly at the ceiling is allowed; one past is not.
	atLimit := strings.Repeat("[", MaxDepth) + strings.Repeat("]", MaxDepth)
	if err := Bounded([]byte(atLimit)); err != nil {
		t.Fatalf("Bounded rejected a blob exactly at MaxDepth: %v", err)
	}
	// Malformed JSON is not this guard's job: it returns nil and lets the
	// caller's own decode surface the parse error.
	if err := Bounded([]byte(`{broken`)); err != nil {
		t.Fatalf("Bounded should defer malformed JSON to the caller, got %v", err)
	}
}
