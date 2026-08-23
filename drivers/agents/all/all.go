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
	registry.Register(claude.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(claude.Class, full), New: newClaude, DefaultConfig: claude.DefaultConfig, ValidateConfig: claude.ValidateConfig, ConfigSchema: json.RawMessage(claude.ConfigSchema)})
	registry.Register(codex.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(codex.Class, full), New: newCodex, DefaultConfig: codex.DefaultConfig, ValidateConfig: codex.ValidateConfig, ConfigSchema: json.RawMessage(codex.ConfigSchema)})
	registry.Register(script.Class, registry.ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementDaemon, Manifest: base.Manifest(script.Class, nil), New: newScript, ValidateConfig: func(raw json.RawMessage) error { _, err := script.ParseConfig(raw); return err }, ConfigSchema: json.RawMessage(script.ConfigSchema)})
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
	cfg.Situation = situation(spec, deps, claude.Class)
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
	cfg.Situation = situation(spec, deps, codex.Class)
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

// situation is where composition — the one place that knows both the instance
// and its host — tells the agent who and where it is. The model has no tool
// that answers either question about itself, so anything not passed here is
// simply unknown to it for the life of the cell.
func situation(spec registry.InstanceSpec, deps registry.Deps, class string) driverproto.Situation {
	out := driverproto.Situation{
		ActorID:      string(spec.ID),
		Class:        class,
		Channel:      string(deps.ChannelID),
		DeviceName:   deps.DeviceName,
		WorkspaceDir: deps.WorkspaceDir,
		IsCore:       deps.ChannelID == channelspec.C0ChannelID,
	}
	if kind, seed, ok := driverproto.ActorSegments(spec.ID); ok {
		out.Kind, out.Seed = string(kind), seed
	}
	return out
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
