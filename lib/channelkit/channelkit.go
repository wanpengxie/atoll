// Package channelkit is the channel template sitting on top of runtime/actorrt.
// It ASSEMBLES a channel's固有 cells (the system actor) and SUBSCRIBES to the
// substrate's obs presence-edge: on a unit's death (the presence DELETED edge)
// it materialises the receiver_unavailable closure. It is a MECHANISM watcher,
// not a Supervisor and not an actor — there is no supervision tree; death is
// obs, and the closure reaction is the only domain duty here. Domain
// coordination is the system actor's job.
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
// only what the closure-doer needs to materialise receiver_unavailable on a
// presence-down edge — and returns storespec.StoredRow so it can read each row's
// Envelope.
type OpenRequestSource interface {
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]storespec.StoredRow, error)
}

// Channel is one assembled channel: actorrt runtime + system cell + the
// presence-down closure wiring (chain + open-request source).
type Channel struct {
	channelID channel.ID
	cells     *actorrt.Runtime
	// deliverer is the confined work-enqueue capability (actorrt hands it out once
	// at New). Routed to the post-harness fanout via Deliverer() — NOT on the
	// broadly-shared Cells() handle, so nothing can inject into a mailbox bypassing
	// the harness.
	deliverer actorrt.Deliverer
	// controller is the confined control-enqueue capability (actorrt hands it out
	// once at New). Exposed via Controller() to the composition root only.
	controller actorrt.Controller

	// Presence-down closure (author #3): on a death edge the watcher writes
	// receiver_unavailable for every in-flight request addressed to the dead
	// actor. nil chain/openReqs → OnDown writes no terminals locally (the dead
	// cell already self-evicted); the caller is responsible for closing the
	// in-flight requests elsewhere.
	chain    rtharness.Writer
	openReqs OpenRequestSource
	clock    func() time.Time

	// logger surfaces closure-drain FAULTS (a swallowed drain failure is a
	// black hole — the loudest thing the watcher can hit). nil → discard.
	logger *slog.Logger
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	// System is the channel's固有 system cell; pass any actorrt.Actor
	// implementation. channelkit assembles cells and does not know domain actor
	// types.
	System actorrt.Actor
	// Chain + OpenRequests wire the presence-down closure (author #3).
	Chain        rtharness.Writer
	OpenRequests OpenRequestSource
	Clock        func() time.Time
	// Logger surfaces closure-drain faults. nil → discard (silent).
	Logger *slog.Logger
}

// New builds the channel, subscribes to the substrate's presence-edge (the
// Channel is the actorrt.PresenceWatcher — see OnDown) and spawns the固有 system
// cell.
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
	// Share the channel's clock + logger with the runtime: the runtime stamps
	// each cell's StartedAt with this clock, so the system actor's uptime
	// (now - StartedAt) stays consistent under a test/non-realtime clock.
	c.cells, c.deliverer, c.controller = actorrt.New(actorrt.Config{Clock: clock, Logger: logger})
	// Register the death watcher BEFORE spawning any cell — no presence-down edge
	// may be missed (closure-critical path).
	c.cells.WatchPresence(c)
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, cfg.System)
	}
	return c
}

// Cells returns the runtime so callers can spawn/address cells in this channel.
// It carries NO enqueue capability — feeding a mailbox is the Deliverer's job
// (Deliverer()), held only by the post-harness fanout.
func (c *Channel) Cells() *actorrt.Runtime { return c.cells }

// Deliverer returns the confined work-enqueue capability — the composition root
// routes it to the post-harness fanout (the sole legitimate mailbox feeder).
func (c *Channel) Deliverer() actorrt.Deliverer { return c.deliverer }

// Controller returns the confined control-enqueue capability — the composition
// root routes it to whoever raises control signals (substrate/composition root).
func (c *Channel) Controller() actorrt.Controller { return c.controller }

// OnDown implements actorrt.PresenceWatcher: death is the DELETED edge of
// presence (obs push), and the Channel is just a subscriber. On the edge it
// materialises receiver_unavailable (the closure REACTION — work that lands in
// truth) for every in-flight request addressed to the dead actor. The terminal
// is SYSTEM-authored — harness Step 8 authorises sender==system +
// reason==receiver_unavailable. This is the substrate's ONLY closure obligation;
// without it a dead cell is a black hole that hangs every waiting caller
// (construction-spec §3.3). Death itself is NOT truth; this reaction is.
//
// It MUST NOT despawn the id: the dead presence has ALREADY self-evicted (the
// runtime's pointer-identity removeIf ran before publishing this edge). A
// watcher Despawn(id) is not pointer-checked, so under same-id replacement it
// would delete/stop the SUCCESSOR — the exact contract PresenceWatcher forbids.
func (c *Channel) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	if c.chain != nil && c.openReqs != nil {
		MaterialiseReceiverUnavailable(ctx, c.logger, c.chain, c.openReqs, c.clock, c.channelID, id)
	}
}

// MaterialiseReceiverUnavailable is the substrate's closure obligation (author
// #3), factored out so that both a local cell death (Channel.OnDown) and any
// remote death edge path materialise the same terminal: for every in-flight
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
		// This is the loudest fault the watcher can hit.
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
