package codex

import (
	"encoding/json"
	"testing"
)

func TestConfigRequiresWorkspaceAndRejectsUnknownKnobs(t *testing.T) {
	if _, err := parseConfig(nil, "", nil); err == nil {
		t.Fatal("missing workspace accepted")
	}
	if _, err := parseConfig(json.RawMessage(`{"unsupported":true}`), "/workspace", nil); err == nil {
		t.Fatal("unknown config knob accepted")
	}
	cfg, err := parseConfig(json.RawMessage(`{}`), "/workspace", nil)
	if err != nil || cfg.Binary != "codex" || cfg.WorkspaceDir != "/workspace" {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
}
