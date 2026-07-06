package app_test

import (
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

// testAgentBuilder is set by a test's setup (before any channel is created); the
// engine catalog classes registered below (go-kimi / claude) delegate to it.
// Engines are FLAT actor classes now, and — since 期10 S5 — agents are
// actorbase.Proc actors (the mailbox loop / turn queue live in agent/base), so
// the builder returns a Proc, not a raw actorrt.Actor. This keeps ALL agent
// injection in test code: the production app builds from the catalog with NO
// test seam (`go test ./app` does not import the real engine providers).
var testAgentBuilder func(chID channel.ID, agentID actor.ActorID) (actorbase.Proc, error)

func init() {
	stub := func(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
		// Capture the builder HERE (the Constructor runs on the reconcile/build
		// path, which Close joins) — not lazily inside New (which runs on the
		// async cell goroutine and would race the cleanup nil-ing the global).
		builder := testAgentBuilder
		if builder == nil {
			return platform.ActorDecl{}, errors.New("test: no agent builder set")
		}
		id := spec.ID
		return platform.ActorDecl{
			ID:   id,
			Kind: actor.KindAgent,
			Factory: platform.ActorFactory{Proc: actorbase.Def{
				Doc: "test agent",
				New: func() (actorbase.Proc, error) {
					return builder(ctx.ChannelID, id)
				},
			}},
		}, nil
	}
	for _, engine := range []string{"go-kimi", "claude"} {
		registry.Register(engine, registry.ClassDecl{Kind: actor.KindAgent, New: stub})
	}
}

// stubAgentFactory builds the e2e stub agent: a Proc that replies "stub-ok" to
// any request — the channel carries a real kind=agent cell (so no-audience
// routing resolves to it) exercising the embedded-cell write path end to end
// without a live LLM. The production built-in is a real go-kimi Proc; the
// topology (channel → route → agent cell → reply in truth) is identical.
func stubAgentFactory(_ channel.ID, _ actor.ActorID) (actorbase.Proc, error) {
	return func(sys actorbase.Sys) error {
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			if msg.Kind == message.KindRequest {
				_, _ = sys.Reply(msg, map[string]any{"text": "stub-ok"})
			}
		}
	}, nil
}
