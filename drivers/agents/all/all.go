// Package all is the sole composition and registration root for Agent
// Drivers. Providers expose immutable factories only and never self-register.
package all

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/claude"
	"github.com/wanpengxie/atoll/drivers/agents/provider/codex"
	"github.com/wanpengxie/atoll/drivers/agents/provider/script"
	agentruntime "github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	full := map[string]bool{driverproto.CapabilitySteer: true, driverproto.CapabilityInterrupt: true, driverproto.CapabilityResume: true}
	registry.Register(claude.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(claude.Class, full), New: newClaude, ValidateConfig: claude.ValidateConfig})
	registry.Register(codex.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(codex.Class, full), New: newCodex, ValidateConfig: codex.ValidateConfig})
	registry.Register(script.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(script.Class, nil), New: newScript, ValidateConfig: func(raw json.RawMessage) error { _, err := script.ParseConfig(raw); return err }})
}

func newClaude(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, errors.New("claude: explicit instance id required")
	}
	if deps.ChannelID == "" {
		return platform.ActorDecl{}, errors.New("claude: channel required")
	}
	cfg, err := claude.ParseConfig(spec.Config, deps.WorkspaceDir, deps.Logger)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("claude config: %w", err)
	}
	return compose(spec, claude.NewProvider(cfg))
}

func newCodex(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, errors.New("codex: explicit instance id required")
	}
	if deps.ChannelID == "" {
		return platform.ActorDecl{}, errors.New("codex: channel required")
	}
	cfg, err := codex.ParseConfig(spec.Config, deps.WorkspaceDir, deps.Logger)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("codex config: %w", err)
	}
	return compose(spec, codex.NewProvider(cfg))
}

func newScript(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, errors.New("script: explicit instance id required")
	}
	cfg, err := script.ParseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return compose(spec, script.NewProviderForTool(cfg.ToolID, cfg.ToolType))
}

func compose(spec registry.InstanceSpec, provider driverproto.Provider) (platform.ActorDecl, error) {
	factory, runtimeSpec, err := agentruntime.Default(provider)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	doc := runtimeSpec.Documentation.SkillDoc
	if doc == "" {
		doc = runtimeSpec.Documentation.Description
	}
	definition, err := base.Def(doc, base.Config{NewRuntime: factory, Runtime: runtimeSpec})
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: definition}}, nil
}
