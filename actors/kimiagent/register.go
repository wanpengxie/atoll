package kimiagent

import (
	"fmt"
	"log"
	"os"

	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("agent", decl) }

// decl: the agent brain — the SINGLE source for both hosts. The daemon builds it
// via BuildAll/RunCompute; the server-embedded built-in builds it via the same
// registry.Build("agent"). Needs a channel + KIMI_* creds, so it is NOT applicable
// (skipped by BuildAll) on a daemon whose --server URL carries no ?channel=.
//
// Situation is derived from Deps: a daemon carries a workspace (exclusive device),
// a server-embedded build does not — so WorkspaceDir presence is the discriminator
// (default-agent-deployment §1.2: server = no exclusive device).
func decl(d registry.Deps) (platform.ActorDecl, bool, error) {
	if d.ChannelID == "" {
		return platform.ActorDecl{}, false, nil // no channel → not applicable here
	}
	sit := Situation{Host: "server"}
	if d.WorkspaceDir != "" {
		sit = Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: d.WorkspaceDir}
	}
	cfg, err := NewConfigFromEnv(BuildSystemPrompt(
		sit, os.Getenv(EnvKeyChannelType), os.Getenv(EnvKeyDomainPrompt)))
	if err != nil {
		return platform.ActorDecl{}, false, fmt.Errorf("config: %w", err)
	}
	agentID := actor.ActorID("agent:main")
	chID := d.ChannelID
	return platform.ActorDecl{
		ID:      agentID,
		Kind:    actor.KindAgent,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor {
			b, err := NewBridge(cfg, agentID, chID, w)
			if err != nil {
				log.Fatalf("daemon: agent bridge: %v", err)
			}
			return b
		},
	}, true, nil
}
