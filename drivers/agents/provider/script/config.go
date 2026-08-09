// Package script is a deterministic provider used by conformance and E2E tests.
package script

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	Class       = "script"
	TypeChat    = "loop.chat"
	TypeVerify  = "loop.verify"
	toolSayType = "echo.say"
	ActorDoc    = "Deterministic scripted assistant: loop.chat calls echo and writes a file; loop.verify reads it."
)

type Config struct {
	ToolID string `json:"tool_id"`
}

func ParseConfig(raw json.RawMessage) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("script: parse config: %w", err)
		}
	}
	c.ToolID = strings.TrimSpace(c.ToolID)
	if c.ToolID == "" {
		return c, fmt.Errorf("script: config.tool_id required")
	}
	return c, nil
}
