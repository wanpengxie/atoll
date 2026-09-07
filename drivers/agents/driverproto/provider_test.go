package driverproto

import "testing"

func TestFallbackOptionsGroupsModelsAndPreservesPairLabels(t *testing.T) {
	selections := []TurnOptions{
		{Model: "m1", Effort: "low"},
		{Model: "m1", Effort: "high"},
		{Model: "m2", Effort: ""},
	}
	titles := []SelectionTitle{
		{Model: "Model One", Effort: "Low"},
		{Model: "Ignored Duplicate Label", Effort: "High"},
		{Model: "Model Two"},
	}
	snapshot := FallbackOptions("test", selections, titles, 1, TurnOptions{})
	if snapshot.Source != OptionsSourceFallback || snapshot.Provider != "test" || snapshot.GeneratedAt == "" {
		t.Fatalf("snapshot metadata=%+v", snapshot)
	}
	if len(snapshot.Models) != 2 || snapshot.Models[0].Value != "m1" || snapshot.Models[0].Label != "Model One" || len(snapshot.Models[0].Efforts) != 2 {
		t.Fatalf("grouped models=%+v", snapshot.Models)
	}
	if snapshot.Default != selections[1] || snapshot.Current != selections[1] {
		t.Fatalf("default/current=%+v/%+v", snapshot.Default, snapshot.Current)
	}
	if !snapshot.Accepts(TurnOptions{Model: "m1", Effort: "high"}) || !snapshot.Accepts(TurnOptions{Model: "m2"}) {
		t.Fatal("advertised selections were rejected")
	}
	if snapshot.Accepts(TurnOptions{Model: "m1", Effort: "future"}) || snapshot.Accepts(TurnOptions{Model: "missing"}) {
		t.Fatal("unadvertised selection was accepted")
	}
}

func TestCloneOptionsSnapshotDoesNotAliasNestedEfforts(t *testing.T) {
	original := OptionsSnapshot{Models: []ModelOption{{Value: "m", Efforts: []EffortOption{{Value: "low"}}}}}
	clone := CloneOptionsSnapshot(original)
	clone.Models[0].Efforts[0].Value = "high"
	if original.Models[0].Efforts[0].Value != "low" {
		t.Fatal("clone mutated source")
	}
}
