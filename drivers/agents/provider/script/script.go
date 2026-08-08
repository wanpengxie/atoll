// Package script is a deterministic provider used by driver conformance tests.
package script

import (
	"encoding/json"
	"fmt"
	"strings"

	agentruntime "github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

const (
	TypeChat    = "loop.chat"
	TypeVerify  = "loop.verify"
	toolSayType = "echo.say"
)
const actorDoc = "Deterministic scripted assistant: loop.chat calls echo and writes a file; loop.verify reads it."

type config struct {
	ToolID string `json:"tool_id"`
}

func init() {
	registry.Register("script", registry.ClassDecl{Kind: actor.KindAgent, New: construct, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }})
}
func parseConfig(raw json.RawMessage) (config, error) {
	var c config
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
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, fmt.Errorf("script: instance id required")
	}
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	def, err := agentruntime.Def(NewProvider(cfg.ToolID))
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: def}}, nil
}
