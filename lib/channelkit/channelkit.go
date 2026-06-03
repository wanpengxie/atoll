// Package channelkit is the channel template — the OTP application+supervisor
// analog sitting on top of runtime/actorrt. It ASSEMBLES a channel's固有 cells
// (the system actor) + the supervision tree. The supervision tree is MECHANISM
// (manages goroutine life + materialises the death-signal closure) — never an
// actor; domain coordination is the system actor's job.
package channelkit

import (
	"context"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// OpenRequestSource provides the set of in-flight requests addressed to a given
// actor (those without a final response). The interface is narrow by design —
// only what the supervisor needs to materialise receiver_unavailable on cell
// death — and returns storespec.StoredRow so the supervisor can read each row's
// Envelope.
type OpenRequestSource interface {
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]storespec.StoredRow, error)
}

// Channel is one assembled channel: actorrt runtime + system cell + the
// death-signal closure wiring (chain + open-request source).
type Channel struct {
	channelID channel.ID
	cells     *actorrt.Runtime

	// Death-signal closure (author #3): on cell death the supervisor writes
	// receiver_unavailable for every in-flight request addressed to the dead
	// actor. nil chain/openReqs → OnDeath only despawns the dead cell and writes
	// no terminals locally; the caller is responsible for closing the
	// death-signal elsewhere.
	chain    rtharness.Writer
	openReqs OpenRequestSource
	clock    func() time.Time

	// logger surfaces closure-drain FAULTS (a swallowed drain failure is a
	// black hole — the loudest thing the supervisor can hit). nil → discard.
	logger *slog.Logger
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	// System is the channel's固有 system cell; pass any actorrt.Actor
	// implementation. channelkit assembles cells and does not know domain actor
	// types.
	System actorrt.Actor
	// Chain + OpenRequests wire the death-signal closure (author #3).
	Chain        rtharness.Writer
	OpenRequests OpenRequestSource
	Clock        func() time.Time
	// Logger surfaces closure-drain faults. nil → discard (silent).
	Logger *slog.Logger
}

// New builds the channel, wires the supervision tree (the Channel is the
// actorrt.Supervisor — see OnDeath) and spawns the固有 system cell.
func New(cfg Config) *Channel {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c := &Channel{
		channelID: cfg.ChannelID,
		chain:     cfg.Chain,
		openReqs:  cfg.OpenRequests,
		clock:     clock,
		logger:    logger,
	}
	c.cells = actorrt.New(actorrt.Config{Supervisor: c})
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, cfg.System)
	}
	return c
}

// Cells returns the runtime so callers can spawn additional cells into this channel.
func (c *Channel) Cells() *actorrt.Runtime { return c.cells }

// OnDeath implements actorrt.Supervisor: it materialises receiver_unavailable
// (closure author #3) for every in-flight request addressed to the dead actor,
// then despawns it. The terminal is SYSTEM-authored — harness Step 8 authorises
// sender==system + reason==receiver_unavailable as the substrate-death author.
// This is the substrate's ONLY closure obligation; without it a dead cell is a
// black hole that hangs every waiting caller (construction-spec §3.3 — the P0
// "Despawn 不收口 / 死 cell 黑洞").
func (c *Channel) OnDeath(ctx context.Context, sig actorrt.DeathSignal) {
	if c.chain != nil && c.openReqs != nil {
		MaterialiseReceiverUnavailable(ctx, c.logger, c.chain, c.openReqs, c.clock, c.channelID, sig.Actor)
	}
	c.cells.Despawn(sig.Actor)
}

// MaterialiseReceiverUnavailable is the substrate's closure obligation (author
// #3), factored out so that both a local cell death (Channel.OnDeath) and any
// remote death-signal path materialise the same terminal: for every in-flight
// request addressed to the dead actor, write a
// SYSTEM-authored receiver_unavailable response into truth. harness Step 8
// authorises sender==system + reason==receiver_unavailable as the substrate
// author. Without this a dead cell — local or across the wire — is a black hole
// that hangs every waiting caller (construction-spec §3.3).
func MaterialiseReceiverUnavailable(ctx context.Context, logger *slog.Logger, chain rtharness.Writer, openReqs OpenRequestSource, clock func() time.Time, channelID channel.ID, dead actor.ActorID) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	reqs, err := openReqs.OpenRequestsForActor(ctx, dead)
	if err != nil {
		// The drain query failed → we cannot close ANY of the dead actor's
		// in-flight requests → every one of its callers is now a black hole.
		// This is the loudest fault the supervisor can hit.
		logger.Error("channelkit.closure.drain_query_failed",
			"channel", channelID, "dead_actor", dead, "err", err)
		return
	}
	sys := message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}
	for i := range reqs {
		req := reqs[i].Envelope
		term, berr := behavior.BuildResponseFromRequest(&req, clock, sys,
			behavior.CorrelationKey(req.ID),
			behavior.ResponseSpec{
				Status: "failed",
				Reason: string(message.TerminalReceiverUnavailable),
			})
		if berr != nil {
			logger.Error("channelkit.closure.build_failed",
				"channel", channelID, "dead_actor", dead, "request", req.ID, "err", berr)
			continue
		}
		cctx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{ActorID: actor.SystemActorID, ChannelID: channelID})
		res, werr := chain.Write(cctx, term)
		if werr != nil {
			logger.Error("channelkit.closure.write_failed",
				"channel", channelID, "dead_actor", dead, "request", req.ID, "err", werr)
			continue
		}
		if res.RejectReason != "" {
			// The harness already logged the reject (harness.write.reject); we
			// add the closure-level fault: this caller stays unclosed.
			logger.Error("channelkit.closure.write_rejected",
				"channel", channelID, "dead_actor", dead, "request", req.ID, "reason", res.RejectReason)
		}
	}
}
