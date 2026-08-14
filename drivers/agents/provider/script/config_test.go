package script

import (
	"encoding/json"
	"testing"
)

func TestParseConfigToolTypeDefaultsAndOverrides(t *testing.T) {
	defaults, err := ParseConfig(json.RawMessage(`{"tool_id":"tool:test"}`))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.ToolType != "echo.say" {
		t.Fatalf("default tool_type=%q", defaults.ToolType)
	}
	override, err := ParseConfig(json.RawMessage(`{"tool_id":"tool:mcp","tool_type":"github.echo"}`))
	if err != nil {
		t.Fatal(err)
	}
	if override.ToolType != "github.echo" {
		t.Fatalf("override tool_type=%q", override.ToolType)
	}
}
