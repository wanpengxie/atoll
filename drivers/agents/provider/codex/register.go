package codex

import (
	"errors"
	"fmt"

	agentruntime "github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

const Class = "codex"

func init() { registry.Register(Class, registry.ClassDecl{Kind: actor.KindAgent, New: newDecl}) }

func newDecl(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, errors.New("codex: explicit instance id required")
	}
	if deps.ChannelID == "" {
		return platform.ActorDecl{}, errors.New("codex: channel required")
	}
	cfg, err := parseConfig(spec.Config, deps.WorkspaceDir, deps.Logger)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("codex config: %w", err)
	}
	def, err := agentruntime.Def(NewProvider(cfg))
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: def}}, nil
}

const agentSkillDoc = "# codex agent\n\nWorkspace-backed assistant using the local Codex app-server."
