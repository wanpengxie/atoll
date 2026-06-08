// Package channelkit is the channel template sitting on top of runtime/actorrt.
// It ASSEMBLES a channel's intrinsic cells (the system actor) and SUBSCRIBES to the
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
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

var sysSender = message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}

// Channel is one assembled channel: actorrt runtime + system cell + the
// presence-down closure wiring (writer + open-request source).
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
	// actor. nil writer/openReqs → OnDown writes no terminals locally (the dead
	// cell already self-evicted); the caller is responsible for closing the
	// in-flight requests elsewhere.
	writer   harness.Writer
	openReqs storespec.MessageQuery
	clock    func() time.Time

	// logger surfaces closure-drain FAULTS (a swallowed drain failure is a
	// black hole — the loudest thing the watcher can hit). nil → discard.
	logger *slog.Logger
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	// System is the channel's intrinsic system cell; pass any actorrt.Actor
	// implementation. channelkit assembles cells and does not know domain actor
	// types.
	System actorrt.Actor
	// Writer + OpenRequests wire the presence-down closure (author #3). Writer is
	// a harness.Writer the composition root injects already stamped with the
	// system caller context.
	Writer       harness.Writer
	OpenRequests storespec.MessageQuery
	Clock        func() time.Time
	// Logger surfaces closure-drain faults. nil → discard (silent).
	Logger *slog.Logger
}

// New builds the channel, subscribes to the substrate's presence-edge (the
// Channel is the actorrt.PresenceWatcher — see OnDown) and spawns the intrinsic
// system cell.
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
		writer:    cfg.Writer,
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
	if c.writer == nil || c.openReqs == nil {
		return
	}
	// Delegate the closure materialisation to the behaviour base (author#3, ONE
	// implementation, co-located with the other two authors — P13). channelkit
	// only injects the seams (runtime writer + store drain) and an onFault log
	// callback; the base holds no logger.
	onFault := func(reqID message.ID, err error) {
		c.logger.Error("channelkit.closure.write_failed",
			"channel", c.channelID, "dead_actor", id, "request", reqID, "err", err)
	}
	if err := behavior.MaterialiseReceiverUnavailable(ctx,
		c.writer, c.openReqs,
		c.clock, sysSender, id, onFault); err != nil {
		// The drain query failed → no caller of the dead actor can be closed →
		// every one is a black hole. The loudest fault the watcher can hit.
		c.logger.Error("channelkit.closure.drain_query_failed",
			"channel", c.channelID, "dead_actor", id, "err", err)
	}
}
