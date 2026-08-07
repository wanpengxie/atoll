package codex

import (
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/drivers/agents/base"
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
	cfg.ActorID = string(spec.ID)
	def, err := base.Def(agentSkillDoc, base.Config{NewEngine: newEngineFn(cfg), SupportedControls: []string{base.TypeSteer, base.TypeInterrupt, base.TypeTerminate, base.TypeRestart}})
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: def}}, nil
}

const agentSkillDoc = "# codex agent\n\nWorkspace-backed assistant using the local Codex app-server. Ordinary non-reserved request types are content; standard agent controls are declared through actor.describe.\n"
