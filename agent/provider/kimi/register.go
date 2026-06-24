package kimi

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// NewDecl: the go-kimi engine's Constructor — its OWN flat actor class
// ("go-kimi", kind=agent), registered directly into the one registry via this
// package's init() (peer to claude and the tool classes; there is no umbrella
// "agent" class). Instantiated under whatever id the spec gives (agent:boost,
// agent:research, …). The id is NOT baked here (actor-instance-model §7):
// default_agent is a name-agnostic pointer, the instance is just another actor.
// The same class yields N agents.
//
// Situation is derived from the host context: a daemon carries a workspace
// (exclusive device), a server-embedded build does not — WorkspaceDir presence
// is the discriminator (default-agent-deployment §1.2).
//
// Config layers env (platform defaults / the server fallback's key) under the
// per-instance spec.Config overlay (channel_actors.config_json — the looper
// self-parses it). Durable resume rides ctx.State (a platform-managed session
// dir + the opaque state slot). A missing channel / id / creds is a hard error —
// the caller (app composition / daemon) decides whether to build.
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
	cfg, err := NewConfigFromSpec(spec.Config, BuildSystemPrompt(
		sit, os.Getenv(EnvKeyChannelType), os.Getenv(EnvKeyDomainPrompt)))
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("config: %w", err)
	}
	// Durable resume seam (agent-spec §三): a platform-managed session dir keeps
	// the looper's opaque session across restarts; the state slot carries the
	// auditable resume pointer (seed read at boot, store written by the looper).
	if ctx.State.Dir != "" {
		cfg.WorkDir = ctx.State.Dir
	}
	cfg.ResumeSeed = ctx.State.Seed
	cfg.Checkpoint = ctx.State.Store
	chID := ctx.ChannelID
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindAgent,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Pen) actorrt.Actor {
			b, err := NewBridge(cfg, id, chID, w)
			if err != nil {
				log.Fatalf("agent bridge: %v", err)
			}
			return b
		},
	}, nil
}
