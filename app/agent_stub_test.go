package app_test

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

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

// stubAgentFactory is the app.AgentFactory the e2e tests inject so every channel
// gets a working default-agent cell without LLM credentials.
func stubAgentFactory(_ channel.ID, agentID actor.ActorID, w harness.Writer) (actorrt.Actor, error) {
	return &stubAgent{w: w, self: agentID}, nil
}
