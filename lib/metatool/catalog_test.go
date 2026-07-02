package metatool_test

import (
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/metatool"
)

func TestFormatCatalogEmpty(t *testing.T) {
	result := metatool.FormatCatalog(introspect.Catalog{})
	actors, ok := result["actors"].([]map[string]any)
	if !ok {
		t.Fatalf("expected actors to be []map[string]any, got %T", result["actors"])
	}
	if len(actors) != 0 {
		t.Fatalf("expected empty actors list, got %d", len(actors))
	}
}

func TestFormatCatalogSortedByID(t *testing.T) {
	catalog := introspect.Catalog{
		Actors: []introspect.CatalogEntry{
			{ID: "tool:zz", Kind: "tool", Present: true},
			{ID: "agent:aa", Kind: "agent", Present: false},
			{ID: "tool:mm", Kind: "tool", Present: true},
		},
	}
	result := metatool.FormatCatalog(catalog)
	actors := result["actors"].([]map[string]any)
	if len(actors) != 3 {
		t.Fatalf("expected 3 actors, got %d", len(actors))
	}
	// Verify sorted by actor_id.
	expected := []string{"agent:aa", "tool:mm", "tool:zz"}
	for i, want := range expected {
		got := actors[i]["actor_id"].(string)
		if got != want {
			t.Fatalf("actors[%d].actor_id = %q, want %q", i, got, want)
		}
	}
}

func TestFormatCatalogFieldsMapped(t *testing.T) {
	catalog := introspect.Catalog{
		Actors: []introspect.CatalogEntry{
			{ID: "tool:xhs", Kind: "tool", Present: true, Binding: "", UptimeMs: 0},
		},
	}
	result := metatool.FormatCatalog(catalog)
	actors := result["actors"].([]map[string]any)
	a := actors[0]
	if a["actor_id"] != "tool:xhs" {
		t.Fatalf("expected actor_id=tool:xhs, got %v", a["actor_id"])
	}
	if a["kind"] != "tool" {
		t.Fatalf("expected kind=tool, got %v", a["kind"])
	}
	if a["present"] != true {
		t.Fatalf("expected present=true, got %v", a["present"])
	}
	// Binding and UptimeMs should be absent when zero-valued.
	if _, ok := a["binding"]; ok {
		t.Fatal("expected binding to be absent when empty")
	}
	if _, ok := a["uptime_ms"]; ok {
		t.Fatal("expected uptime_ms to be absent when zero")
	}
}

func TestFormatCatalogBindingAndUptimeIncluded(t *testing.T) {
	catalog := introspect.Catalog{
		Actors: []introspect.CatalogEntry{
			{ID: "tool:xhs", Kind: "tool", Present: true, Binding: "daemon:abc", UptimeMs: 12345},
		},
	}
	result := metatool.FormatCatalog(catalog)
	actors := result["actors"].([]map[string]any)
	a := actors[0]
	if a["binding"] != "daemon:abc" {
		t.Fatalf("expected binding=daemon:abc, got %v", a["binding"])
	}
	if a["uptime_ms"] != int64(12345) {
		t.Fatalf("expected uptime_ms=12345, got %v (%T)", a["uptime_ms"], a["uptime_ms"])
	}
}
