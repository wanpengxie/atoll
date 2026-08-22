package claude

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestConfigRequiresWorkspaceAndRejectsUnknownKnobs(t *testing.T) {
	if _, err := ParseConfig(nil, "", nil); err == nil {
		t.Fatal("empty workspace accepted")
	}
	if _, err := ParseConfig(json.RawMessage(`{"unsupported":true}`), "/workspace", nil); err == nil {
		t.Fatal("unknown config field accepted")
	}
	cfg, err := ParseConfig(json.RawMessage(`{"model":"sonnet"}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Binary != "claude" || cfg.Model != "sonnet" || cfg.processFactory == nil {
		t.Fatalf("cfg=%+v", cfg)
	}
}

func TestConfigPublishesConfiguredSelections(t *testing.T) {
	cfg, err := ParseConfig(json.RawMessage(`{"selections":[{"model":"claude-test","effort":"low"},{"model":"claude-test","effort":"high"}],"default":1}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := NewProvider(cfg).Spec()
	want := driverproto.TurnOptions{Model: "claude-test", Effort: "high"}
	if len(spec.Selections) != 2 || spec.Selections[1] != want || spec.DefaultSelection != 1 {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestConfigSelectionLabelsRideBesideOptionsNotInside(t *testing.T) {
	cfg, err := ParseConfig(json.RawMessage(`{"selections":[{"model":"claude-test","effort":"low","model_label":"Test","effort_label":"低"},{"model":"claude-test","effort":"high"}]}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := NewProvider(cfg).Spec()
	if len(spec.SelectionTitles) != 2 || spec.SelectionTitles[0] != (driverproto.SelectionTitle{Model: "Test", Effort: "低"}) || spec.SelectionTitles[1] != (driverproto.SelectionTitle{}) {
		t.Fatalf("titles=%+v", spec.SelectionTitles)
	}
	// Options identity must stay label-free (labels never enter persistence
	// or equality).
	if spec.Selections[0] != (driverproto.TurnOptions{Model: "claude-test", Effort: "low"}) {
		t.Fatalf("selections=%+v", spec.Selections)
	}
}

func TestConfigRejectsDuplicateOrBlankSelections(t *testing.T) {
	// A duplicate (model, effort) pair becomes two identical oneOf branches,
	// making the fully valid submit match both and fail oneOf validation.
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"m","effort":"e"},{"model":"m","effort":"e"}]}`)); err == nil {
		t.Fatal("duplicate selection accepted")
	}
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"m","effort":""}]}`)); err == nil {
		t.Fatal("blank effort accepted")
	}
}

func TestSpawnArgsGolden(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		session string
		resume  bool
		want    []string
	}{
		{name: "new session", session: "new", want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--setting-sources", "", "--strict-mcp-config", "--session-id", "new"}},
		{name: "resume", session: "old", resume: true, want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--setting-sources", "", "--strict-mcp-config", "--resume", "old"}},
		{name: "model", cfg: Config{Model: "opus"}, session: "new", want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--setting-sources", "", "--strict-mcp-config", "--session-id", "new", "--model", "opus"}},
		// The decl prompt rides on Claude Code's own system prompt; user
		// settings (plugins, hooks, CLAUDE.md, user MCP) are never loaded.
		{name: "prompt", cfg: Config{Prompt: "You are the Steward."}, session: "new", want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--setting-sources", "", "--strict-mcp-config", "--append-system-prompt", "You are the Steward.", "--session-id", "new"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := spawnArgs(test.cfg, test.session, test.resume, driverproto.TurnOptions{}); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("args=%q want=%q", got, test.want)
			}
		})
	}
}
