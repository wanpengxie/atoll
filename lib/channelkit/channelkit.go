// Package channelkit is the channel template — the OTP application+supervisor
// analog sitting on top of runtime/actorrt. It ASSEMBLES a channel's固有 cells
// (the system actor) + the audience policy resolver + the supervision tree.
// The supervision tree is MECHANISM (manages goroutine life + materialises the
// death-signal closure) — never an actor; domain coordination is the system
// actor's job.
package channelkit

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/policy"
	"github.com/wanpengxie/ActOS/lib/sysactor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// OpenRequestSource queries a dead actor's in-flight requests (those without a
// final response). runtime/store.Messages implements it (OpenRequestsForActor),
// so the supervisor can materialise receiver_unavailable for each on cell death.
type OpenRequestSource interface {
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID, limit int) ([]message.Envelope, error)
}

// Channel is one assembled channel: actorrt runtime + system cell + policy +
// the death-signal closure wiring (chain + open-request source).
type Channel struct {
	channelID channel.ID
	cells     *actorrt.Runtime
	system    *sysactor.SystemActor
	resolver  *policy.Resolver

	// Death-signal closure (author #3): on cell death the supervisor writes
	// receiver_unavailable for every in-flight request addressed to the dead
	// actor. nil chain/openReqs (e.g. a pure compute host with no local truth)
	// → OnDeath only despawns; death is materialised at the home via DeathFrame.
	chain    harness.Chain
	openReqs OpenRequestSource
	clock    func() time.Time
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	System    *sysactor.SystemActor
	// Chain + OpenRequests wire the death-signal closure (author #3).
	Chain        harness.Chain
	OpenRequests OpenRequestSource
	Clock        func() time.Time
}

// New builds the channel, wires the supervision tree (the Channel is the
// actorrt.Supervisor — see OnDeath) and spawns the固有 system cell.
func New(cfg Config) *Channel {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	c := &Channel{
		channelID: cfg.ChannelID,
		system:    cfg.System,
		resolver:  policy.New(),
		chain:     cfg.Chain,
		openReqs:  cfg.OpenRequests,
		clock:     clock,
	}
	c.cells = actorrt.New(actorrt.Config{Supervisor: c})
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, cfg.System)
	}
	return c
}

// Cells exposes the runtime so the deployment layer spawns business cells.
func (c *Channel) Cells() *actorrt.Runtime { return c.cells }

// Resolver exposes the audience policy resolver.
func (c *Channel) Resolver() *policy.Resolver { return c.resolver }

// OnDeath implements actorrt.Supervisor: it materialises receiver_unavailable
// (closure author #3) for every in-flight request addressed to the dead actor,
// then despawns it. The terminal is SYSTEM-authored — harness Step 8 authorises
// sender==system + reason==receiver_unavailable as the substrate-death author.
// This is the substrate's ONLY closure obligation; without it a dead cell is a
// black hole that hangs every waiting caller (construction-spec §3.3 — the P0
// "Despawn 不收口 / 死 cell 黑洞").
func (c *Channel) OnDeath(ctx context.Context, sig actorrt.DeathSignal) {
	if c.chain != nil && c.openReqs != nil {
		if reqs, err := c.openReqs.OpenRequestsForActor(ctx, sig.Actor, 0); err == nil {
			sys := message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
			for i := range reqs {
				req := reqs[i]
				term, berr := behavior.BuildResponseFromRequest(&req, c.clock, sys,
					behavior.CorrelationKey(req.ID),
					behavior.ResponseSpec{
						Status: "failed",
						Reason: string(message.TerminalReceiverUnavailable),
					})
				if berr != nil {
					continue
				}
				// Step 8 authorises this (substrate-death author). Best-effort:
				// a concurrent receiver final / duplicate is absorbed by Step 8.
				_, _ = c.chain.Write(ctx, term)
			}
		}
	}
	c.cells.Despawn(sig.Actor)
}
