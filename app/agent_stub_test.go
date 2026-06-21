package app_test

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// testAgentBuilder is set by a test's setup (before any channel is created); the
// engine catalog classes registered below (go-kimi / claude) delegate to it.
// Engines are FLAT actor classes now (agent-kind-vs-class): boost runs "go-kimi"
// and tests create agents with looper "claude"/"go-kimi", so the per-channel
// engine (channel_actors.class) resolves to one of these. This keeps ALL agent
// injection in test code: the production app builds from the catalog
// (registry.Build(<engine class>)) with NO test seam — `go test ./app` does not
// import the real engine providers (wired at cmd/server), so tests own these.
var testAgentBuilder func(chID channel.ID, agentID actor.ActorID, w harness.Writer) (actorrt.Actor, error)

func init() {
	stub := func(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
		if testAgentBuilder == nil {
			return platform.ActorDecl{}, errors.New("test: no agent builder set")
		}
		id := spec.ID
		return platform.ActorDecl{
			ID:      id,
			Kind:    actor.KindAgent,
			Binding: actor.BindingRuntimeOutbound,
			Factory: func(w harness.Writer) actorrt.Actor {
				impl, err := testAgentBuilder(ctx.ChannelID, id, w)
				if err != nil {
					return nil
				}
				return impl
			},
		}, nil
	}
	for _, engine := range []string{"go-kimi", "claude"} {
		registry.Register(engine, stub)
	}
}

// stubAgent is a minimal default-agent cell for e2e: the channel carries a real
// kind=agent cell (so no-audience routing resolves to it) and, on a request, it
// replies "stub-ok" — exercising the embedded-cell write path end to end without
// a live LLM. The production built-in is a real go-kimi Bridge; the topology
// (channel → route → agent cell → reply in truth) is identical.
type stubAgent struct {
	w    harness.Writer
	self actor.ActorID
}

func (s *stubAgent) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind == message.KindRequest {
		_, _ = behavior.RespondJSON(ctx, s.w, time.Now, env,
			message.Sender{Kind: actor.KindAgent, ID: s.self},
			map[string]any{"text": "stub-ok"})
	}
	return nil
}

// stubAgentFactory builds the e2e stub agent. Tests assign it to testAgentBuilder
// so every channel gets a working default-agent cell without LLM credentials.
func stubAgentFactory(_ channel.ID, agentID actor.ActorID, w harness.Writer) (actorrt.Actor, error) {
	return &stubAgent{w: w, self: agentID}, nil
}
