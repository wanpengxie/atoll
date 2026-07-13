package channelkit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	// actor — but ONLY once closedForever confirms the id can never answer again
	// (deregistered / never a member). A nil closure triple (systemPen/openReqs/
	// closedForever) → OnDown writes no terminals locally; the caller closes the
	// in-flight requests elsewhere.
	systemPen     harness.Pen
	openReqs      storespec.MessageQuery
	closedForever behavior.ClosedForever
	clock         func() time.Time

	// logger surfaces closure-drain FAULTS (a swallowed drain failure is a
	// black hole — the loudest thing the watcher can hit). nil → discard.
	logger *slog.Logger

	// downCh is the O(1) hand-off the down edge (OnDown) posts a dead id onto —
	// the publishDown contract is "watcher MUST be non-blocking", and the closure
	// materialisation (a store scan + terminal writes) is decidedly NOT: it must
	// not run on the dying goroutine's reap path (G0-3). A bounded buffer (a
	// full one drops + logs — the level scan is the load-bearing backstop, the
	// edge is a lossy fast-path, P0-3); consumeDown drains it serially.
	downCh    chan actor.ActorID
	downStop  chan struct{}
	downDone  chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	system    func(*actorrt.Runtime, actorrt.Incarnation) actorrt.Actor
	started   atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	leaked    atomic.Int64
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
	// ClosedForever is the monotone closure predicate the composition root injects
	// (registry: !ok || !IsActive → closed). channelkit closes a receiver's callers
	// ONLY on this fact — deregistered / never a member, the states that can never
	// gain a successor. A crashed-but-registered receiver is left to the deadline
	// reaper (its callers wait for the request TTL). channelkit holds no registry
	// itself: whose liveness/membership counts is the assembler's word, not the
	// channel wiring's. Part of the closure triple — all of {SystemPen,
	// OpenRequests, ClosedForever} or none.
	ClosedForever behavior.ClosedForever
	Clock         func() time.Time
	// Logger surfaces closure-drain faults. nil → discard (silent).
	Logger *slog.Logger
}

// New builds the inert channel wiring and subscribes to the substrate's down
// edge. Start later births the intrinsic system cell and closure consumer, so
// the composition root can finish cross-component wiring first.
func New(cfg Config) (*Channel, error) {
	// Closure is a three-seam capability: the writer (SystemPen), the open-request
	// source (OpenRequests) and the monotone closed-forever predicate
	// (ClosedForever). All three or none — a partial wiring would leave the death
	// edge/level scan unable to author terminals, a silent black hole for every
	// caller of a departed actor. Refuse it at assembly rather than discover it as
	// a hang in production.
	set := 0
	if cfg.SystemPen != nil {
		set++
	}
	if cfg.OpenRequests != nil {
		set++
	}
	if cfg.ClosedForever != nil {
		set++
	}
	if set != 0 && set != 3 {
		return nil, fmt.Errorf("channelkit: closure wiring incomplete — SystemPen, OpenRequests and ClosedForever are all-or-nothing")
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Channel{
		channelID:     cfg.ChannelID,
		systemPen:     cfg.SystemPen,
		openReqs:      cfg.OpenRequests,
		closedForever: cfg.ClosedForever,
		clock:         clock,
		logger:        logger,
		downCh:        make(chan actor.ActorID, downChBuffer),
		downStop:      make(chan struct{}),
		downDone:      make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		closeDone:     make(chan struct{}),
		system:        cfg.System,
	}
	// Share the channel's clock + logger with the runtime: the runtime stamps
	// each cell's StartedAt with this clock, so the system actor's uptime
	// (now - StartedAt) stays consistent under a test/non-realtime clock.
	c.cells, c.deliverer = actorrt.New(actorrt.Config{Clock: clock, Logger: logger})
	// Register the death watcher BEFORE Start can spawn any cell — no down edge
	// may be missed (closure-critical path).
	c.cells.WatchDown(c)
	// Retain the intrinsic system-cell factory for Start. The resulting cell uses the
	// RAW system pen (anchor not wrapped in a livePen): the system actor
	// writes singleton SystemActorID terminals, has no successor principal to
	// impersonate, and the closure reconciler must write even when no cell is live
	// — gating it would defeat it. So no incarnation is welded here.
	return c, nil
}

// Start births the intrinsic anchor and then starts the closure consumer.
func (c *Channel) Start() error {
	if !c.started.CompareAndSwap(false, true) {
		panic("channelkit: Channel.Start called twice")
	}
	if c.system != nil {
		// Singleton invariant made explicit (was prose only): the system anchor is
		// the SOLE occupant of SystemActorID and is minted exactly once, here, at
		// channel assembly. SpawnIfAbsent + created-assert fails fast if the reserved
		// id is ever already occupied (a second anchor spawn / a member admission
		// leaking the reserved id) instead of Attach's silent last-go-live replace.
		if _, created, err := c.cells.SpawnIfAbsent(actor.SystemActorID, actor.KindSystem, func(inc actorrt.Incarnation) actorrt.Actor {
			return c.system(c.cells, inc)
		}); err != nil {
			c.started.Store(false) // Close after a failed Start must not wait on an unstarted consumer.
			return err
		} else if !created {
			panic("channelkit: system anchor invariant violated — SystemActorID already occupied at assembly")
		}
	}
	// Start the resident closure consumer: OnDown only O(1)-posts a dead id; this
	// goroutine drains the buffer serially and does the blocking closure work off
	// the dying goroutine's reap path (G0-3). Started after the runtime exists
	// (it reads liveness for the recheck) and joined by Close in Home.Close's
	// teardown序 (after cells stop, before stores close).
	go c.consumeDown()
	return nil
}

// Cells returns the runtime so callers can spawn/address cells in this channel.
// It carries NO enqueue capability — feeding a mailbox is the Deliverer's job
// (Deliverer()), held only by the post-harness fanout.
func (c *Channel) Cells() *actorrt.Runtime { return c.cells }

// Deliverer returns the confined work-enqueue capability — the composition root
// routes it to the post-harness fanout (the sole legitimate mailbox feeder).
func (c *Channel) Deliverer() actorrt.Deliverer { return c.deliverer }

// OnDown implements actorrt.DownWatcher: death is the DELETED edge of
// the embodiment (obs push), and the Channel is just a subscriber. The edge is a
// TRIGGER: it hands the dead id to the resident consumer, which — only if the id
// is closed forever (deregistered / never a member) — materialises
// receiver_unavailable (the closure REACTION — work that lands in truth) for every
// in-flight request addressed to it. The terminal is SYSTEM-authored — the harness
// authorises sender==system + reason==receiver_unavailable. This is the
// substrate's ONLY closure obligation; without it a departed member is a black
// hole that hangs every waiting caller. Death itself is NOT truth; this reaction
// is. A crash whose member is still registered writes NO terminal here — its
// callers wait for the request deadline (a successor may yet answer).
//
// It MUST NOT despawn the id: the dead embodiment has ALREADY self-evicted (the
// runtime's pointer-identity removeIf ran before publishing this edge). A
// watcher Despawn(id) is not pointer-checked, so under same-id replacement it
// would delete/stop the SUCCESSOR — the exact contract DownWatcher forbids.
func (c *Channel) OnDown(ctx context.Context, id actor.ActorID, _ actorrt.Incarnation, cause error) {
	if c.systemPen == nil || c.openReqs == nil || c.closedForever == nil {
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
// does the blocking closure work off the death-edge reap path. Started by Start,
// joined by Close. The death edge is only a TRIGGER: closeFor re-derives the
// monotone closed-forever fact before authoring anything, so an edge for an id
// that is still a registered member (a crash whose successor may yet take over)
// writes no terminal — exactly as the level scan judges.
func (c *Channel) consumeDown() {
	defer close(c.downDone)
	for {
		select {
		case <-c.downStop:
			return
		default:
		}
		select {
		case <-c.downStop:
			return
		case id := <-c.downCh:
			c.closeFor(c.ctx, id)
		}
	}
}

// closeFor materialises receiver_unavailable for every in-flight request
// addressed to id — ONLY when id is CLOSED FOREVER (deregistered / never a
// member), the monotone fact that guarantees no successor will ever answer. A
// still-registered id (a crashed instance that may be re-embodied) is left to
// the request deadline — its callers wait for the TTL, never mis-closed. A
// predicate-lookup failure skips this round entirely (never a false close); the
// level scan is the backstop. System-authored: identity rides the system pen.
func (c *Channel) closeFor(ctx context.Context, id actor.ActorID) {
	gone, err := c.closedForever(ctx, id)
	if err != nil {
		// The lookup failed → do NOT close (a lookup failure is never a dereg). The
		// lossy edge is abandoned; the level scan retries next tick.
		c.logger.Error("channelkit.closure.predicate_failed",
			"channel", c.channelID, "dead_actor", id, "err", err)
		return
	}
	if !gone {
		return // still a registered member — the edge is a crash, not a corpse; the deadline closes it.
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

// Close joins the resident closure consumer — the down-edge goroutine's teardown
// seam Home.Close drives AFTER cells stop (no more edges will be produced) and
// BEFORE the stores close (a late materialise must not write into a closing
// store). Idempotent-safe: Home.Close calls it exactly once. Any id still
// buffered at Close is abandoned to the level scan (the same lossy-edge / level-
// backstop split as a full-buffer drop).
func (c *Channel) Close() {
	c.closeWithin(5 * time.Second)
}

func (c *Channel) closeWithin(timeout time.Duration) {
	c.closeOnce.Do(func() {
		defer close(c.closeDone)
		if !c.started.Load() {
			c.cancel()
			return
		}
		// Signal-then-join BEFORE cancel: closing downStop lets consumeDown finish
		// whatever closeFor call is already in flight under a STILL-LIVE ctx, then
		// notice the stop signal and return on its own next loop iteration. Only
		// once it has actually joined (or the bounded timeout gives up on it) do we
		// cancel c.ctx. Cancelling first (the prior order) raced an in-flight
		// closeFor's predicate/drain queries against ctx.Done() on every routine
		// shutdown, misreporting the loudest closure fault
		// (predicate_failed/drain_query_failed — "every caller of the dead actor is
		// a black hole") for what is just an ordinary Close, not a real fault.
		close(c.downStop)
		select {
		case <-c.downDone:
			c.cancel()
		case <-time.After(timeout):
			// Bounded abandon proof: the only write is idempotent UNIQUE-backed
			// closure materialisation (or a loud closed-store error); this consumer
			// owns no Spawn/Fork/Attach or goroutine-production capability.
			c.leaked.Add(1)
			c.logger.Error("channelkit.close.join_timeout", "timeout", timeout,
				"safety", "writes are idempotent and no actors are produced")
			c.cancel()
		}
	})
	<-c.closeDone
}

func (c *Channel) Leaked() int64 { return c.leaked.Load() }

// Reconcile runs the closure level scan (the reconciler): for every
// receiver that still holds an open request and is CLOSED FOREVER (deregistered /
// never a member), materialise receiver_unavailable. This is the level-triggered
// correctness backstop the death edge alone cannot give — a lost edge (an edge
// dropped from a full buffer, an open request predating a home restart, a dereg
// whose edge never fired) leaves an orphan that only a scan can close. Idempotent:
// re-writing a terminal collides with the per-request UNIQUE index, so repeated
// scans produce no duplicate closure. The composition root drives it at startup
// and on a low-frequency ticker; the death edge (OnDown) remains the lossy
// fast-path that closes the common case immediately without waiting for the next
// scan.
//
// A receiver merely absent from liveness (crashed, not yet placed) is NOT closed
// here — still a registered member, it may get a successor, so its stranded
// requests are the deadline reaper's to close, not this scan's.
//
// nil closure triple → no closure capability injected → no-op (mirrors OnDown).
func (c *Channel) Reconcile(ctx context.Context) {
	if c.systemPen == nil || c.openReqs == nil || c.closedForever == nil {
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
		c.systemPen, c.openReqs, c.closedForever,
		c.clock, onFault); err != nil {
		// The distinct-receivers scan failed → no orphan can be enumerated →
		// every absent receiver's callers stay black holes until the next scan.
		c.logger.Error("channelkit.closure.reconcile_scan_failed",
			"channel", c.channelID, "err", err)
	}
}
