package actorrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ErrMailboxFull is returned by Deliver when the actor's bounded mailbox is
// full. The caller MUST NOT block: a full request mailbox is rejected at the
// seam and the sender's caller-scoped closure (or the substrate down edge)
// collapses it. An event whose mailbox is full may be dropped (the log is truth).
var ErrMailboxFull = errors.New("actorrt: mailbox full")

// ErrCellStopped is returned by Deliver after the cell has begun teardown.
var ErrCellStopped = errors.New("actorrt: cell stopped")

// cell is one actor INSTANCE: its impl (private state), a single goroutine, and
// a bounded envelope mailbox. The mailbox carries ONLY envelopes — the
// substrate's isolation guarantee is that the sole way to affect an actor is to
// send it a message; there is no path to run caller-supplied code on the cell
// goroutine.
type cell struct {
	id       actor.ActorID
	mintKind actor.Kind
	impl     Actor
	inbox chan *message.Envelope

	// started is the substrate-stamped bind instant — the obs `uptime` fact's
	// authoritative source (uptime = now - started, derived by the consumer).
	// Only the substrate can produce it (Erlang /proc starttime model), so it is
	// stamped at Spawn, never self-reported by the actor.
	started time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// onDown publishes the obs down edge (death) for this id. The
	// runtime wires it to its down-watch fanout. Death is an obs push, not a
	// control signal and not truth — the watcher's reaction (receiver_unavailable)
	// is the part that becomes work/truth.
	onDown func(actor.ActorID, error)
	// onObs publishes an actor-produced obs snapshot (obs push/actor) to the
	// runtime's per-actor obs watchers. Invoked from the actor via
	// ActorContext.PublishObs. It passes THIS cell as the self pointer so the
	// runtime can pointer-identity-gate the fanout (a stale predecessor that
	// outlived its incarnation cannot publish obs attributed to a same-id
	// successor — the ABA guard, mirroring onExit). No watcher → no-op.
	onObs func(actor.ActorID, embodiment, ObsKind, ObsValue)
	// logger surfaces cell-lifecycle edge observability.
	logger *slog.Logger
	// onExit is the pointer-identity-checked self-eviction hook the Runtime injects:
	// on ANY exit (clean or panic) the cell removes itself from the addressing
	// map IFF the map still points to this very instance. This is how death
	// makes an instance unaddressable WITHOUT the cell trying to stop/join
	// itself (a goroutine cannot join itself — that was the death deadlock).
	onExit func(actor.ActorID, embodiment)
	// onReap strikes this body off the runtime's zombie ledger — invoked from the
	// cell goroutine's临终 defer the instant it truly exits (account⇔residue:
	// an entry exists IFF a physical corpse remains). Runs AFTER onExit (which
	// enrols the natural-death zombie) and impl.Stop (whose block would keep this
	// body a leaked zombie until it finally returns).
	onReap func(embodiment)

	// live is the per-incarnation WHEN-validity atomic: false at allocShell, true
	// at go-live (register), false again on any teardown (death / stop / eviction).
	// IsLive reads it LOCK-FREE so a livePen can fence a dangling capability per
	// write without serialising on the runtime's addressing lock.
	live atomic.Bool

	// done is closed when the cell goroutine has fully exited (after Stop).
	done chan struct{}

	stopOnce sync.Once
	// closed guards inbox-send so Deliver never enqueues into a torn-down cell.
	mu     sync.Mutex
	closed bool
}

// allocShell allocates a cell SHELL (impl=nil, live=false) — phase 1 of the
// two-phase Spawn. The returned pointer is the incarnation's stable p;
// Spawn fills c.impl from the build closure (OUTSIDE the lock, while IsLive is
// still false), then flips live true at go-live. mailbox is the bounded inbox
// depth; onExit is the self-eviction hook; onDown publishes the death (embodiment
// DELETED) edge; started is the substrate-stamped bind instant (obs uptime);
// kind is the out-generation attribute welded at mint (Spawn/SpawnIfAbsent/
// Fork's caller-held kind), read back via Runtime.Stat (UnitStat.Kind).
func allocShell(parent context.Context, id actor.ActorID, kind actor.Kind, mailbox int, onDown func(actor.ActorID, error), onObs func(actor.ActorID, embodiment, ObsKind, ObsValue), onExit func(actor.ActorID, embodiment), onReap func(embodiment), started time.Time, logger *slog.Logger) *cell {
	if mailbox <= 0 {
		mailbox = 64
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(parent)
	return &cell{
		id:       id,
		mintKind: kind,
		inbox:    make(chan *message.Envelope, mailbox),
		started:  started,
		ctx:      ctx,
		cancel:   cancel,
		onDown:   onDown,
		onObs:    onObs,
		logger:   logger,
		onExit:   onExit,
		onReap:   onReap,
		done:     make(chan struct{}),
	}
}

// Self implements ActorContext.
func (c *cell) Self() actor.ActorID { return c.id }

// startedAt implements embodiment: the substrate-stamped bind instant (obs uptime
// source). The substrate is the authority for it — the cell never self-reports.
func (c *cell) startedAt() time.Time { return c.started }

// kind implements embodiment: the out-generation attribute welded at mint time.
func (c *cell) kind() actor.Kind { return c.mintKind }

// isLive implements embodiment: the lock-free WHEN-validity probe (per-incarnation
// atomic). True only between go-live and teardown.
func (c *cell) isLive() bool { return c.live.Load() }

// markDead implements embodiment: flip the liveness atomic to false (idempotent).
func (c *cell) markDead() { c.live.Store(false) }

// doneCh implements embodiment: the channel closed once the cell goroutine has
// fully exited (the zombie escort / DrainZombies join handle).
func (c *cell) doneCh() <-chan struct{} { return c.done }

// PublishObs implements ActorContext: the actor's obs PUSH/producer end. It
// hands an opaque snapshot to the runtime's per-actor obs fanout (no watcher →
// no-op). This is NOT a self-send and NOT truth — it publishes observable state,
// the substrate forwards it without interpreting.
func (c *cell) PublishObs(kind ObsKind, val ObsValue) {
	if c.onObs != nil {
		c.onObs(c.id, c, kind, val)
	}
}

// Deliver is the substrate enqueue path into this cell's mailbox — held only by
// the post-harness fanout (and the wire-dispatch arm), never exposed to the
// actor itself (ActorContext has no self-send). It never blocks: a full mailbox
// returns ErrMailboxFull, a stopped cell ErrCellStopped.
func (c *cell) Deliver(env *message.Envelope) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCellStopped
	}
	c.mu.Unlock()

	select {
	case c.inbox <- env:
		return nil
	default:
		return ErrMailboxFull
	}
}

// start spawns the cell goroutine. It calls Start (if implemented), then loops
// over the mailbox invoking Receive serially, then on teardown self-evicts,
// publishes the down edge (on abnormal death), and calls Stop. A
// panic anywhere is recovered and surfaced as that death edge.
func (c *cell) start() {
	go func() {
		defer close(c.done)
		// Reap AFTER the death defer below (LIFO: it runs first) — so the natural-
		// death zombie onExit enrols is struck only once the body has truly finished
		// (past impl.Stop, whose block keeps this corpse a leaked zombie until it
		// returns). Runs before close(c.done), so any escort/DrainZombies waiter that
		// wakes on done sees a consistent already-reaped ledger.
		defer func() {
			if c.onReap != nil {
				c.onReap(c)
			}
		}()

		var deathCause error
		defer func() {
			if r := recover(); r != nil {
				deathCause = fmt.Errorf("actorrt: cell %s panicked: %v", c.id, r)
			}
			// (a-1) Flip liveness off FIRST so a livePen fences this incarnation the
			// instant the goroutine begins to unwind (before self-eviction / death
			// edge). markDead is idempotent; onExit/Despawn may flip it again.
			c.live.Store(false)
			// (a0) Cancel the cell ctx so EVERY in-flight reqCtx cascades cancelled
			// — the dead instance's downstream goroutines (tools / LLM / sub-awaits
			// holding a reqCtx) unwind instead of leaking. cancel() is idempotent;
			// a clean stop() already cancelled, an abnormal death cancels here.
			c.cancel()
			// (a) Make this instance unaddressable FIRST — pointer-identity
			// self-eviction. Never self-stop()/join: a goroutine cannot wait on
			// its own exit.
			if c.onExit != nil {
				c.onExit(c.id, c)
			}
			// (b) On abnormal death, PUBLISH the down edge (obs push).
			// The runtime fans it out to embodiment watchers (e.g. the closure-doer
			// that materialises receiver_unavailable). onDown guards each watcher
			// against panic; death is positively-observed, the substrate never
			// guesses "slow".
			if deathCause != nil && c.onDown != nil {
				c.onDown(c.id, deathCause)
			}
			// (c) Best-effort resource release.
			if st, ok := c.impl.(Stopper); ok {
				_ = st.Stop(context.Background())
			}
		}()

		if starter, ok := c.impl.(Starter); ok {
			if err := starter.Start(c.ctx, c); err != nil {
				deathCause = fmt.Errorf("actorrt: cell %s Start failed: %w", c.id, err)
				return
			}
		}

		// dying is the optional occupant exit-signal arm (DownReporter). A
		// non-implementing occupant leaves it nil — a nil channel never becomes
		// ready in a select, so the arm below is simply disabled and the loop's
		// behaviour is unchanged from before this hook existed.
		var dying <-chan error
		if dr, ok := c.impl.(DownReporter); ok {
			dying = dr.Dying()
		}

		for {
			select {
			case <-c.ctx.Done():
				// Non-stopping arbitration (§1.4) prefers a dying signal that raced
				// ctx.Done() to plain readiness: re-check dying NON-BLOCKINGLY before
				// returning, so a simultaneously-ready exit code is not lost to
				// select's random choice between two ready cases.
				if err, ok := drainDying(dying); ok {
					deathCause = c.arbitrateDeath(err)
				}
				return
			case err := <-dying:
				deathCause = c.arbitrateDeath(err)
				return
			case env := <-c.inbox:
				c.safeReceive(env)
			}
		}
	}()
}

// drainDying is a non-blocking read of the optional dying arm — nil-channel
// safe (a nil channel's receive in a select-with-default never fires, so a
// non-DownReporter occupant reports ok=false here).
func drainDying(dying <-chan error) (err error, ok bool) {
	if dying == nil {
		return nil, false
	}
	select {
	case err = <-dying:
		return err, true
	default:
		return nil, false
	}
}

// arbitrateDeath applies the ②>① priority (opus review, §1.4): regulation ②
// (stopping position pre-empts everything) wins over the occupant's own exit
// code — a worker's ANY return during Draining (external Stop/Despawn/replace
// in flight) is forced quiet, because the natural graceful-shutdown write is
// `return ctx.Err()`, which regulation ① alone would misjudge loud. Outside
// stopping, the occupant's code stands: nil is quiet, non-nil is loud.
func (c *cell) arbitrateDeath(err error) error {
	if c.isStopping() {
		return nil
	}
	return err
}

// isStopping reports whether external teardown (initiateStop) has already
// begun — the Draining entry condition (§1.4) the death-arbitration priority
// consults. Shares c.closed, the same flag Deliver gates on (both ask "has
// teardown started", the one existing signal for that position).
func (c *cell) isStopping() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// safeReceive invokes impl.Receive under the cell's LIFE ctx (c.ctx). 期10 S5
// retired the per-request reqCtx machine (inflight/armRequest/requestCtx): the
// ONLY cell occupant now is the actorbase engine, whose pump does NOT consult
// this ctx (its serve/call ledgers are the sole authority for what ctx a
// delivered Msg carries — spec §5 red line) and whose RequestCanceller hook
// handles the request-cancel signal — so the cell no longer derives a
// per-request deadline/cancel it would only feed to a ctx the engine never
// reads. Cell death (c.cancel) still cascades into everything derived from c.ctx.
//
// A panic propagates naturally up the cell goroutine stack to the deferred
// recover in start() (one death per cell, not per message) — safeReceive itself
// does not recover. A returned (non-panic) error is NOT a death — closure
// belongs to the sender — so it is swallowed for observability.
func (c *cell) safeReceive(env *message.Envelope) {
	if err := c.impl.Receive(c.ctx, env); err != nil {
		_ = err
	}
}

// cancelRequest implements embodiment: deliver the request-cancel signal for id,
// off the cell goroutine, as a ONE-HOP handoff to the occupant's RequestCanceller
// (dispatch/disposition split — the occupant decides what cancelling id means to
// its own in-flight work). 期10 S5 retired the built-in reqCtx fallback (the
// per-request cancel machine): the sole occupant is the actorbase engine, which
// implements RequestCanceller (engine.CancelRequest → serve-ledger close), so a
// non-implementing occupant is a best-effort no-op — the caller's own closure
// owns the terminal, the cancel is only a hint.
func (c *cell) cancelRequest(id message.ID) {
	if rc, ok := c.impl.(RequestCanceller); ok {
		rc.CancelRequest(id)
	}
}

// initiateStop implements embodiment: the non-blocking, idempotent SIGNAL half of
// teardown — trigger death and return at once, WITHOUT joining c.done. It is the
// only teardown signal now (the join is the escort's job, bounded by grace) —
// stopOnce makes it safe to call any number of times, and c.done is safe to read
// from multiple goroutines (the escort / DrainZombies).
//
// It calls onExit IMMEDIATELY (mirroring port.die()'s existing onExit call) so
// a cascaded child is removed from r.embodiments/LiveIDs() the instant this is
// called, not lazily whenever its own goroutine notices ctx cancellation — a
// dying parent's cascade (removeIf) calls this, and a child's initiateStop
// synchronously re-enters onExit/removeIf (which takes r.mu) — safe because
// removeIf calls this AFTER releasing r.mu.
//
// onExit/removeIf is already pointer-identity-idempotent (deletes IFF
// r.embodiments[id]==self), so the cell's own goroutine later reaching its death
// defer (cell.go start()'s onExit call) and calling onExit a SECOND time is a
// safe no-op — the entry is already gone. impl.Stop()'s resource release still
// runs on that natural exit path, unaffected by the early table removal here.
func (c *cell) initiateStop() {
	c.stopOnce.Do(func() {
		c.live.Store(false) // fence any welded cap at once; the goroutine's defer also flips it.
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
		if c.onExit != nil {
			c.onExit(c.id, c) // immediate table removal — do not wait for the goroutine to exit.
		}
	})
}

// beginTeardown implements embodiment: synchronously mark the quiet-teardown
// intent (c.closed) so a racing self-death arbitrates quiet (isStopping→true).
// Non-blocking — no cancel/closeConn/evict (the escort's signalDespawn does those).
func (c *cell) beginTeardown() {
	c.live.Store(false)
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// signalDespawn implements embodiment: a cell has NO wire, so the despawn-flavoured
// SIGNAL half is identical to initiateStop (there is no remote to send a
// KindDespawn frame to — the KindDespawn arm is the port's alone). dl is unused
// (no frame write to bound). The distinction exists only so the by-name
// termination entries reach a port's wire signal; a cell collapses it. Non-joining
// — the escort watches doneCh bounded by grace, no goroutine ever self-joins.
func (c *cell) signalDespawn(_ context.Context) { c.initiateStop() }
