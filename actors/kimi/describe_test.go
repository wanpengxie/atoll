package kimi

import "testing"

func TestTypeMetadataIncludesPendingBudget(t *testing.T) {
	meta, ok := TypeMetadata[TypeCommand]
	if !ok {
		t.Fatalf("missing metadata for %s", TypeCommand)
	}
	if len(meta.AllowedKinds) != 1 || meta.AllowedKinds[0] != "request" {
		t.Fatalf("allowed_kinds = %v; want [request]", meta.AllowedKinds)
	}
	if meta.MaxPendingMs != DefaultMaxPendingMs {
		t.Fatalf("max_pending_ms = %d; want %d", meta.MaxPendingMs, DefaultMaxPendingMs)
	}
	if meta.Description == "" {
		t.Fatal("description should not be empty")
	}
}
