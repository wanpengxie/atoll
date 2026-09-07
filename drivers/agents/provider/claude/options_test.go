package claude

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestInitializeOptionsMapsAliasAndKeepsPerModelEfforts(t *testing.T) {
	w := &worker{cfg: Config{
		Selections:   []driverproto.TurnOptions{{Model: "fable", Effort: "high"}},
		versionProbe: func(string) string { return "2.1.258" },
		latestProbe:  func() string { return "2.2.0" },
	}}
	raw := json.RawMessage(`{"models":[
		{"value":"claude-fable-5[1m]","displayName":"Fable","description":"hard tasks","supportsEffort":true,"supportedEffortLevels":["low","high"]},
		{"value":"haiku","displayName":"Haiku","supportsEffort":false}
	]}`)
	snapshot := w.optionsFromInitialize(raw, driverproto.TurnOptions{Model: "fable", Effort: "high"})
	if snapshot.Source != driverproto.OptionsSourceNative || len(snapshot.Models) != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	want := driverproto.TurnOptions{Model: "claude-fable-5[1m]", Effort: "high"}
	if snapshot.Current != want || len(snapshot.Models[0].Efforts) != 2 || len(snapshot.Models[1].Efforts) != 0 {
		t.Fatalf("current/models=%+v/%+v", snapshot.Current, snapshot.Models)
	}
	if snapshot.Client.Current != "2.1.258" || snapshot.Client.Latest != "2.2.0" || snapshot.Client.UpdateStatus != driverproto.UpdateAvailable {
		t.Fatalf("client=%+v", snapshot.Client)
	}
}

func TestInitializeOptionsFallsBackWhenNativeCatalogIsUnavailable(t *testing.T) {
	w := &worker{cfg: Config{
		Selections:      []driverproto.TurnOptions{{Model: "configured", Effort: "medium"}},
		SelectionTitles: []driverproto.SelectionTitle{{Model: "Configured", Effort: "Medium"}},
		versionProbe:    func(string) string { return "2.1.258" },
	}}
	snapshot := w.optionsFromInitialize(json.RawMessage(`{"models":[]}`), driverproto.TurnOptions{})
	if snapshot.Source != driverproto.OptionsSourceFallback || len(snapshot.Models) != 1 || snapshot.Current.Model != "configured" {
		t.Fatalf("fallback=%+v", snapshot)
	}
}

func TestInitializeOptionsDropsConfiguredEffortForModelWithoutEffort(t *testing.T) {
	w := &worker{cfg: Config{
		Selections: []driverproto.TurnOptions{{Model: "haiku", Effort: "low"}},
	}}
	raw := json.RawMessage(`{"models":[
		{"value":"haiku","displayName":"Haiku","supportsEffort":false},
		{"value":"sonnet","displayName":"Sonnet","supportsEffort":true,"supportedEffortLevels":["low","high"]}
	]}`)
	snapshot := w.optionsFromInitialize(raw, driverproto.TurnOptions{Model: "haiku", Effort: "low"})
	want := driverproto.TurnOptions{Model: "haiku"}
	if snapshot.Current != want {
		t.Fatalf("current=%+v want=%+v", snapshot.Current, want)
	}
}

func TestClaudeSelectionClearsEffortForModelWithoutEffort(t *testing.T) {
	catalog := driverproto.OptionsSnapshot{Models: []driverproto.ModelOption{
		{Value: "sonnet", Efforts: []driverproto.EffortOption{{Value: "high"}}},
		{Value: "haiku"},
	}}
	got := claudeSelectionForCatalog(
		driverproto.TurnOptions{Model: "sonnet", Effort: "high"},
		driverproto.TurnOptions{Model: "haiku"},
		catalog,
	)
	if got != (driverproto.TurnOptions{Model: "haiku"}) {
		t.Fatalf("selection=%+v", got)
	}
}
