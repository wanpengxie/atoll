package kimi

import (
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// NewDecl: the go-kimi engine's Constructor — its OWN flat actor class
// ("go-kimi", kind=agent). Instantiated under whatever id the spec gives
// (agent:boost, agent:research, …); the id is NOT baked (default_agent is a
// name-agnostic pointer). Situation is host-derived (a daemon carries a
// workspace, a server-embedded build does not).
//
// Config layers env DEFAULTS under the per-instance spec.Config overlay
// (channel_composition.config_json). It builds the agent as a base.Def (a Proc over
// agent/base's skeleton), NOT a raw actorrt.Actor (期10 S5: the mailbox loop /
// turn queue / response分拣 live in the base). A missing channel / id / creds is
// a hard error.
func NewDecl(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	if ctx.ChannelID == "" {
		return platform.ActorDecl{}, errors.New("agent: requires a channel")
	}
	id := spec.ID
	if id == "" {
		return platform.ActorDecl{}, errors.New("agent: requires an explicit instance id")
	}
	sit := Situation{Host: "server"}
	if ctx.WorkspaceDir != "" {
		sit = Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: ctx.WorkspaceDir}
	}
	cfg, err := NewConfigFromSpec(spec.Config, sit)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	def, err := base.Def(agentSkillDoc, base.Config{
		NewEngine: newEngineFn(cfg, defaultAgentFactory),
	})
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("kimi agent def: %w", err)
	}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindAgent,
		Factory: platform.ActorFactory{Proc: def},
	}, nil
}
