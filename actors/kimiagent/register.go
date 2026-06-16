package kimiagent

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("agent", construct) }

// construct: the agent brain — ONE class ("agent"), instantiated under whatever
// id the spec gives (agent:boost, agent:research, …). The id is NOT baked here
// (actor-instance-model §7): default_agent is a name-agnostic pointer, the
// instance is just another actor. The same class yields N agents.
//
// Situation is derived from the host context: a daemon carries a workspace
// (exclusive device), a server-embedded build does not — WorkspaceDir presence
// is the discriminator (default-agent-deployment §1.2).
//
// Config day-0 still comes from env (KIMI_*); per-instance persona/config from
// spec.Config is the additive next step (D6). A missing channel / id / creds is
// a hard error — the caller (app composition / daemon) decides whether to build.
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
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
	cfg, err := NewConfigFromEnv(BuildSystemPrompt(
		sit, os.Getenv(EnvKeyChannelType), os.Getenv(EnvKeyDomainPrompt)))
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	chID := ctx.ChannelID
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindAgent,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			b, err := NewBridge(cfg, id, chID, w)
			if err != nil {
				log.Fatalf("agent bridge: %v", err)
			}
			return b
		},
	}, nil
}
