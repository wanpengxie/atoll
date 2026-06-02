// Package agentactor hosts a worker subprocess session as a kind=agent actor
// cell. The worker session (spawn / lease / IPC — runtime/workerhost) is the
// actor's RESOURCE; the cell goroutine owns the session handle with no lock.
// Receive translates an inbound request into a worker trigger; the worker's
// report is emitted back as the response (wired by the daemon host that owns
// the workerhost). v2: runs on compute (daemon host) — agent actors spawn their
// worker there, not on the channel home.
package agentactor

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TriggerFunc launches/continues the worker session for an inbound request. The
// daemon host injects an implementation backed by runtime/workerhost.
type TriggerFunc func(ctx context.Context, env *message.Envelope) error

// AgentActor is the worker-session facade.
type AgentActor struct {
	self      actor.ActorID
	channelID channel.ID
	chain     harness.Chain
	lookup    message.RequestLookup
	clock     func() time.Time
	trigger   TriggerFunc
}

// Deps bundles the channel services + the worker trigger seam.
type Deps struct {
	Self      actor.ActorID
	ChannelID channel.ID
	Chain     harness.Chain
	Lookup    message.RequestLookup
	Clock     func() time.Time
	Trigger   TriggerFunc
}

// New constructs an agent actor cell.
func New(deps Deps) *AgentActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &AgentActor{
		self: deps.Self, channelID: deps.ChannelID, chain: deps.Chain,
		lookup: deps.Lookup, clock: clock, trigger: deps.Trigger,
	}
}

// Receive routes an inbound request to the worker session (serial on the cell
// goroutine). Non-requests are ignored at this seam.
func (a *AgentActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind == message.KindRequest && a.trigger != nil {
		return a.trigger(ctx, env)
	}
	return nil
}
