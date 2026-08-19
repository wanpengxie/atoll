// Package script is a deterministic provider used by conformance and E2E tests.
package script

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	Class           = "script"
	TypeAsk         = "agent.ask"
	defaultToolType = "echo.say"
	ActorDoc        = "Deterministic scripted assistant: agent.ask calls echo and writes a file, or verifies a named resource."
)

type Config struct {
	ToolID   string `json:"tool_id"`
	ToolType string `json:"tool_type"`
}

func ParseConfig(raw json.RawMessage) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("script: parse config: %w", err)
		}
	}
	c.ToolID = strings.TrimSpace(c.ToolID)
	c.ToolType = strings.TrimSpace(c.ToolType)
	if c.ToolID == "" {
		return c, fmt.Errorf("script: config.tool_id required")
	}
	if c.ToolType == "" {
		c.ToolType = defaultToolType
	}
	return c, nil
}

// ConfigSchema publishes what Config above accepts.
const ConfigSchema = `{
  "type": "object",
  "properties": {
    "tool_id": {"type": "string"},
    "tool_type": {"type": "string"}
  }
}`
