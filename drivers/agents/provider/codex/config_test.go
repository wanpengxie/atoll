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
