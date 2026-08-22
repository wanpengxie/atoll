package codex

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestConfigRequiresWorkspaceAndRejectsUnknownKnobs(t *testing.T) {
	if _, err := ParseConfig(nil, "", nil); err == nil {
		t.Fatal("missing workspace accepted")
	}
	if _, err := ParseConfig(json.RawMessage(`{"unsupported":true}`), "/workspace", nil); err == nil {
		t.Fatal("unknown config knob accepted")
	}
	cfg, err := ParseConfig(json.RawMessage(`{}`), "/workspace", nil)
	if err != nil || cfg.Binary != "codex" || cfg.WorkspaceDir != "/workspace" {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
}

func TestConfigPublishesConfiguredSelections(t *testing.T) {
	cfg, err := ParseConfig(json.RawMessage(`{"selections":[{"model":"gpt-test","effort":"low"},{"model":"gpt-test","effort":"high"}],"default":1}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := NewProvider(cfg).Spec()
	want := driverproto.TurnOptions{Model: "gpt-test", Effort: "high"}
	if len(spec.Selections) != 2 || spec.Selections[1] != want || spec.DefaultSelection != 1 {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestConfigSelectionLabelsRideBesideOptionsNotInside(t *testing.T) {
	cfg, err := ParseConfig(json.RawMessage(`{"selections":[{"model":"gpt-test","effort":"low","model_label":"Test","effort_label":"低"}]}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := NewProvider(cfg).Spec()
	if len(spec.SelectionTitles) != 1 || spec.SelectionTitles[0] != (driverproto.SelectionTitle{Model: "Test", Effort: "低"}) {
		t.Fatalf("titles=%+v", spec.SelectionTitles)
	}
	if spec.Selections[0] != (driverproto.TurnOptions{Model: "gpt-test", Effort: "low"}) {
		t.Fatalf("selections=%+v", spec.Selections)
	}
}

func TestConfigRejectsDuplicateOrBlankSelections(t *testing.T) {
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"m","effort":"e"},{"model":"m","effort":"e"}]}`)); err == nil {
		t.Fatal("duplicate selection accepted")
	}
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"","effort":"e"}]}`)); err == nil {
		t.Fatal("blank model accepted")
	}
}
