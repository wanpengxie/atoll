package channelkit

import (
	"context"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
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
	// System builds the channel's intrinsic system cell GIVEN the live runtime.
	// It is a factory, not a ready cell, because the system actor legitimately
	// reads the runtime (presence Stat) — born before the runtime, it would need
	// a back-filled pointer. channelkit builds the runtime first, then calls this
	// with the real *actorrt.Runtime, so the cell holds a live reference at
	// construction (no back-fill, no construction cycle). channelkit assembles
	// cells and does not know domain actor types. nil → no system cell.
	System func(rt *actorrt.Runtime) actorrt.Actor
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
// system cell. Assembly order: runtime → watcher → system cell, so the System
// factory receives the live runtime (no back-filled presence pointer).
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
	c.cells, c.deliverer = actorrt.New(actorrt.Config{Clock: clock, Logger: logger})
	// Register the death watcher BEFORE spawning any cell — no presence-down edge
	// may be missed (closure-critical path).
	c.cells.WatchPresence(c)
	// Spawn the intrinsic system cell, built against the live runtime.
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, cfg.System(c.cells))
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
	// Inject CallerContext for the system-authored write: the harness requires
	// a CallerContext on every write (step 0 rejects without one). The death
	// closure is system-authored (sender == SystemActorID), so the caller
	// principal is the system actor.
	ctx = harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   actor.SystemActorID,
		ChannelID: c.channelID,
	})
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

// presenceProbe adapts the channel's runtime presence map to the behaviour
// reconciler's PresenceProbe seam: present = a live instance hosted right now
// (Stat's is_process_alive authority). Absent = closure owed.
type presenceProbe struct{ rt *actorrt.Runtime }

func (p presenceProbe) Present(id actor.ActorID) bool {
	_, ok := p.rt.Stat(id)
	return ok
}

// Reconcile runs the closure level scan (author #3 reconciler): for every
// receiver that still holds an open request and is currently ABSENT from the
// substrate presence map, materialise receiver_unavailable. This is the
// level-triggered correctness backstop the death edge alone cannot give — a lost
// edge (clean despawn, ctx-cancel, an open request predating a home restart)
// leaves an orphan that only a scan can close. Idempotent: re-writing a terminal
// collides with the per-request UNIQUE index, so repeated scans produce no
// duplicate closure. The composition root drives it at startup and on a
// low-frequency ticker; the death edge (OnDown) remains the lossy fast-path that
// closes the common case immediately without waiting for the next scan.
//
// nil writer/openReqs → no closure capability injected → no-op (mirrors OnDown).
func (c *Channel) Reconcile(ctx context.Context) {
	if c.writer == nil || c.openReqs == nil {
		return
	}
	ctx = harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:   actor.SystemActorID,
		ChannelID: c.channelID,
	})
	onFault := func(reqID message.ID, err error) {
		c.logger.Error("channelkit.closure.reconcile_fault",
			"channel", c.channelID, "request", reqID, "err", err)
	}
	if err := behavior.ReconcileReceiverUnavailable(ctx,
		c.writer, c.openReqs, presenceProbe{rt: c.cells},
		c.clock, sysSender, onFault); err != nil {
		// The distinct-receivers scan failed → no orphan can be enumerated →
		// every absent receiver's callers stay black holes until the next scan.
		c.logger.Error("channelkit.closure.reconcile_scan_failed",
			"channel", c.channelID, "err", err)
	}
}
