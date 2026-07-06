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

	// downCh is the O(1) hand-off the down edge (OnDown) posts a dead id onto —
	// the publishDown contract is "watcher MUST be non-blocking", and the closure
	// materialisation (a store scan + terminal writes) is decidedly NOT: it must
	// not run on the dying goroutine's reap path (G0-3). A bounded buffer (a
	// full one drops + logs — the level scan is the load-bearing backstop, the
	// edge is a lossy fast-path, P0-3); consumeDown drains it serially.
	downCh   chan actor.ActorID
	downStop chan struct{}
	downDone chan struct{}
}

// downChBuffer bounds the death-edge hand-off queue. A full buffer drops the
// edge (logged) — the 30s level scan + startup first-scan close whatever the
// edge lost, idempotently (the per-request UNIQUE index makes a re-scan a no-op).
const downChBuffer = 64

// Config assembles a channel.
type Config struct {
	ChannelID channel.ID
	// System builds the channel's intrinsic system cell GIVEN the live runtime.
	// It is a factory, not a ready cell, because the system actor legitimately
	// reads the runtime (liveness Stat) — born before the runtime, it would need
	// a back-filled pointer. channelkit builds the runtime first, then calls this
	// with the real *actorrt.Runtime, so the cell holds a live reference at
	// construction (no back-fill, no construction cycle). channelkit assembles
	// cells and does not know domain actor types. nil → no system cell. The
	// factory also receives the cell's own incarnation (from the SpawnIfAbsent
	// build closure) so the composition root can weld an incarnation-owned Spawn
	// arm onto the anchor.
	System func(rt *actorrt.Runtime, inc actorrt.Incarnation) actorrt.Actor
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
		downCh:    make(chan actor.ActorID, downChBuffer),
		downStop:  make(chan struct{}),
		downDone:  make(chan struct{}),
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
		// Singleton invariant made explicit (was prose only): the system anchor is
		// the SOLE occupant of SystemActorID and is minted exactly once, here, at
		// channel assembly. SpawnIfAbsent + created-assert fails fast if the reserved
		// id is ever already occupied (a second anchor spawn / a member admission
		// leaking the reserved id) instead of Spawn's silent last-go-live replace.
		if _, created := c.cells.SpawnIfAbsent(actor.SystemActorID, actor.KindSystem, func(inc actorrt.Incarnation) actorrt.Actor {
			return cfg.System(c.cells, inc)
		}); !created {
			panic("channelkit: system anchor invariant violated — SystemActorID already occupied at assembly")
		}
	}
	// Start the resident closure consumer: OnDown only O(1)-posts a dead id; this
	// goroutine drains the buffer serially and does the blocking closure work off
	// the dying goroutine's reap path (G0-3). Started after the runtime exists
	// (it reads liveness for the recheck) and joined by Stop in Home.Close's
	// teardown序 (after cells stop, before stores close).
	go c.consumeDown()
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
	// O(1) hand-off ONLY (G0-3): the closure materialisation (a store scan +
	// terminal writes) violates publishDown's "watcher MUST be non-blocking"
	// contract and must not run on the dying goroutine's reap path. Post the dead
	// id to the resident consumer; a full buffer is dropped + logged (the level
	// scan is the load-bearing backstop — the edge is a lossy fast-path, P0-3).
	select {
	case c.downCh <- id:
	default:
		c.logger.Warn("channelkit.closure.edge_dropped",
			"channel", c.channelID, "dead_actor", id,
			"note", "down buffer full; level scan will close orphans")
	}
}

// consumeDown is the resident closure goroutine: it drains dead ids serially and
// does the blocking closure work off the death-edge reap path. Started by New,
// joined by Stop. Before materialising, it RE-CHECKS liveness (Present) — the
// async hand-off widens the window in which a same-id successor may have taken
// over, so the edge path must guard against mis-closing the successor's callers
// exactly as the level scan does.
func (c *Channel) consumeDown() {
	defer close(c.downDone)
	probe := livenessProbe{rt: c.cells}
	for {
		select {
		case id := <-c.downCh:
			c.closeFor(context.Background(), id, probe)
		case <-c.downStop:
			return
		}
	}
}

// closeFor materialises receiver_unavailable for every in-flight request
// addressed to id — UNLESS a same-id successor is already live (Present), in
// which case the edge is stale and skipped (its callers are the successor's, not
// black holes). System-authored: identity rides the system pen.
func (c *Channel) closeFor(ctx context.Context, id actor.ActorID, probe livenessProbe) {
	if probe.Present(id) {
		return // a successor took over between the edge and this consume — not a corpse.
	}
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

// Stop joins the resident closure consumer — the down-edge goroutine's teardown
// seam Home.Close drives AFTER cells stop (no more edges will be produced) and
// BEFORE the stores close (a late materialise must not write into a closing
// store). Idempotent-safe: Home.Close calls it exactly once. Any id still
// buffered at Stop is abandoned to the level scan (the same lossy-edge / level-
// backstop split as a full-buffer drop).
func (c *Channel) Stop() {
	close(c.downStop)
	<-c.downDone
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
