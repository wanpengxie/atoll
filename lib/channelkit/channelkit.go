package channelkit

import (
	"context"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Channel is one assembled channel: actorrt runtime + system cell + the
// death-edge closure wiring (writer + open-request source).
type Channel struct {
	channelID channel.ID
	cells     *actorrt.Runtime
	// deliverer is the confined work-enqueue capability (actorrt hands it out once
	// at New). Routed to the post-harness fanout via Deliverer() — NOT on the
	// broadly-shared Cells() handle, so nothing can inject into a mailbox bypassing
	// the harness.
	deliverer actorrt.Deliverer

	// Death-edge closure: on a death edge the watcher writes
	// receiver_unavailable for every in-flight request addressed to the dead
	// actor. nil systemPen/openReqs → OnDown writes no terminals locally (the dead
	// cell already self-evicted); the caller is responsible for closing the
	// in-flight requests elsewhere.
	systemPen harness.Pen
	openReqs  storespec.MessageQuery
	clock     func() time.Time

	// logger surfaces closure-drain FAULTS (a swallowed drain failure is a
	// black hole — the loudest thing the watcher can hit). nil → discard.
	logger *slog.Logger
}

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	// System builds the channel's intrinsic system cell GIVEN the live runtime.
	// It is a factory, not a ready cell, because the system actor legitimately
	// reads the runtime (liveness Stat) — born before the runtime, it would need
	// a back-filled pointer. channelkit builds the runtime first, then calls this
	// with the real *actorrt.Runtime, so the cell holds a live reference at
	// construction (no back-fill, no construction cycle). channelkit assembles
	// cells and does not know domain actor types. nil → no system cell.
	System func(rt *actorrt.Runtime) actorrt.Actor
	// SystemPen + OpenRequests wire the death-edge closure. SystemPen is
	// the system Pen the composition root injects (Mint(SystemActorID, chID)): the
	// system identity is welded into the pen, so the closure's terminals are system-
	// authored by construction — channelkit never stamps a caller itself.
	SystemPen    harness.Pen
	OpenRequests storespec.MessageQuery
	Clock        func() time.Time
	// Logger surfaces closure-drain faults. nil → discard (silent).
	Logger *slog.Logger
}

// New builds the channel, subscribes to the substrate's down edge (the
// Channel is the actorrt.DownWatcher — see OnDown) and spawns the intrinsic
// system cell. Assembly order: runtime → watcher → system cell, so the System
// factory receives the live runtime (no back-filled runtime pointer).
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
		systemPen: cfg.SystemPen,
		openReqs:  cfg.OpenRequests,
		clock:     clock,
		logger:    logger,
	}
	// Share the channel's clock + logger with the runtime: the runtime stamps
	// each cell's StartedAt with this clock, so the system actor's uptime
	// (now - StartedAt) stays consistent under a test/non-realtime clock.
	c.cells, c.deliverer = actorrt.New(actorrt.Config{Clock: clock, Logger: logger})
	// Register the death watcher BEFORE spawning any cell — no down edge
	// may be missed (closure-critical path).
	c.cells.WatchDown(c)
	// Spawn the intrinsic system cell, built against the live runtime. It uses the
	// RAW system pen (anchor not wrapped in a livePen): the system actor
	// writes singleton SystemActorID terminals, has no successor principal to
	// impersonate, and the closure reconciler must write even when no cell is live
	// — gating it would defeat it. So no incarnation is welded here.
	if cfg.System != nil {
		c.cells.Spawn(actor.SystemActorID, actor.KindSystem, func(actorrt.Incarnation) actorrt.Actor {
			return cfg.System(c.cells)
		})
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

// OnDown implements actorrt.DownWatcher: death is the DELETED edge of
// the embodiment (obs push), and the Channel is just a subscriber. On the edge it
// materialises receiver_unavailable (the closure REACTION — work that lands in
// truth) for every in-flight request addressed to the dead actor. The terminal
// is SYSTEM-authored — the harness authorises sender==system +
// reason==receiver_unavailable. This is the substrate's ONLY closure obligation;
// without it a dead cell is a black hole that hangs every waiting caller.
// Death itself is NOT truth; this reaction is.
//
// It MUST NOT despawn the id: the dead embodiment has ALREADY self-evicted (the
// runtime's pointer-identity removeIf ran before publishing this edge). A
// watcher Despawn(id) is not pointer-checked, so under same-id replacement it
// would delete/stop the SUCCESSOR — the exact contract DownWatcher forbids.
func (c *Channel) OnDown(ctx context.Context, id actor.ActorID, cause error) {
	if c.systemPen == nil || c.openReqs == nil {
		return
	}
	// The death closure is system-authored: the injected writer is the system Pen
	// (Mint(SystemActorID)), which welds sender==SystemActorID + the channel id
	// into every write. No caller injection here — identity rides the pen.
	//
	// Delegate the closure materialisation to the behaviour base (the single
	// implementation, co-located with its counterparts). channelkit
	// only injects the seams (system pen + store drain) and an onFault log
	// callback; the base holds no logger.
	onFault := func(reqID message.ID, err error) {
		c.logger.Error("channelkit.closure.write_failed",
			"channel", c.channelID, "dead_actor", id, "request", reqID, "err", err)
	}
	if err := behavior.MaterialiseReceiverUnavailable(ctx,
		c.systemPen, c.openReqs,
		c.clock, id, onFault); err != nil {
		// The drain query failed → no caller of the dead actor can be closed →
		// every one is a black hole. The loudest fault the watcher can hit.
		c.logger.Error("channelkit.closure.drain_query_failed",
			"channel", c.channelID, "dead_actor", id, "err", err)
	}
}

// livenessProbe adapts the channel's runtime liveness view (Stat) to the behaviour
// reconciler's LivenessProbe seam: present = a live instance hosted right now
// (Stat's is_process_alive authority). Absent = closure owed.
type livenessProbe struct{ rt *actorrt.Runtime }

func (p livenessProbe) Present(id actor.ActorID) bool {
	_, ok := p.rt.Stat(id)
	return ok
}

// Reconcile runs the closure level scan (the reconciler): for every
// receiver that still holds an open request and is currently ABSENT from the
// substrate liveness view, materialise receiver_unavailable. This is the
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
	if c.systemPen == nil || c.openReqs == nil {
		return
	}
	// System-authored (same as OnDown): the injected writer is the system Pen, so
	// the reconciler's terminals carry sender==SystemActorID by construction. No
	// caller injection — identity rides the pen.
	onFault := func(reqID message.ID, err error) {
		c.logger.Error("channelkit.closure.reconcile_fault",
			"channel", c.channelID, "request", reqID, "err", err)
	}
	if err := behavior.ReconcileReceiverUnavailable(ctx,
		c.systemPen, c.openReqs, livenessProbe{rt: c.cells},
		c.clock, onFault); err != nil {
		// The distinct-receivers scan failed → no orphan can be enumerated →
		// every absent receiver's callers stay black holes until the next scan.
		c.logger.Error("channelkit.closure.reconcile_scan_failed",
			"channel", c.channelID, "err", err)
	}
}
