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

// decl: the agent brain (fat-daemon host of the same Bridge the server spawns as
// the built-in fallback). Needs a channel + KIMI_* creds, so it is NOT applicable
// (skipped by BuildAll) on a daemon whose --server URL carries no ?channel=.
func decl(d registry.Deps) (platform.ActorDecl, bool, error) {
	if d.ChannelID == "" {
		return platform.ActorDecl{}, false, nil // no channel → not applicable here
	}
	cfg, err := NewConfigFromEnv(BuildSystemPrompt(
		Situation{Host: "daemon", HasWorkspace: true, WorkspaceDir: d.WorkspaceDir},
		os.Getenv(EnvKeyChannelType), os.Getenv(EnvKeyDomainPrompt)))
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
