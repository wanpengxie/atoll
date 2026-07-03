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
	id    actor.ActorID
	impl  Actor
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

	// inflight maps a live request's id to its per-request cancel. Each Receive
	// runs under a reqCtx derived from c.ctx (deadline from ExpiresAt, else plain
	// cancel); the entry is recorded before the call and removed when it closes.
	// This is the request-scope of cancel(scope): an off-loop fire (a cross-wire
	// KindCancel) cancels exactly the one Receive holding the goroutine without
	// queuing behind it. Built WITH its collapse — the entry is deleted the
	// instant the request closes, so the table never outlives its requests.
	flightMu sync.Mutex
	inflight map[message.ID]context.CancelFunc
}

// allocShell allocates a cell SHELL (impl=nil, live=false) — phase 1 of the
// two-phase Spawn. The returned pointer is the incarnation's stable p;
// Spawn fills c.impl from the build closure (OUTSIDE the lock, while IsLive is
// still false), then flips live true at go-live. mailbox is the bounded inbox
// depth; onExit is the self-eviction hook; onDown publishes the death (embodiment
// DELETED) edge; started is the substrate-stamped bind instant (obs uptime).
func allocShell(parent context.Context, id actor.ActorID, mailbox int, onDown func(actor.ActorID, error), onObs func(actor.ActorID, embodiment, ObsKind, ObsValue), onExit func(actor.ActorID, embodiment), started time.Time, logger *slog.Logger) *cell {
	if mailbox <= 0 {
		mailbox = 64
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(parent)
	return &cell{
		id:       id,
		inbox:    make(chan *message.Envelope, mailbox),
		started:  started,
		ctx:      ctx,
		cancel:   cancel,
		onDown:   onDown,
		onObs:    onObs,
		logger:   logger,
		onExit:   onExit,
		done:     make(chan struct{}),
		inflight: make(map[message.ID]context.CancelFunc),
	}
}

// Self implements ActorContext.
func (c *cell) Self() actor.ActorID { return c.id }

// startedAt implements embodiment: the substrate-stamped bind instant (obs uptime
// source). The substrate is the authority for it — the cell never self-reports.
func (c *cell) startedAt() time.Time { return c.started }

// isLive implements embodiment: the lock-free WHEN-validity probe (per-incarnation
// atomic). True only between go-live and teardown.
func (c *cell) isLive() bool { return c.live.Load() }

// markDead implements embodiment: flip the liveness atomic to false (idempotent).
func (c *cell) markDead() { c.live.Store(false) }

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

		for {
			select {
			case <-c.ctx.Done():
				return
			case env := <-c.inbox:
				c.safeReceive(env)
			}
		}
	}()
}

// safeReceive invokes impl.Receive under a per-request ctx derived from c.ctx.
// The reqCtx is the request-scope of cancel(scope): it carries the ExpiresAt
// deadline (so an expired request's downstream work unwinds at its instant) and
// is the handle an off-loop cancel (caller abandon / cross-wire) fires to
// interrupt exactly this Receive. The serial model is unchanged — one reqCtx is
// live at a time; it is a per-call scope, not concurrency.
//
// A panic propagates naturally up the cell goroutine stack to the deferred
// recover in start() (one death per cell, not per message) — safeReceive itself
// does not recover, but its deferred reqCancel + table removal still run on the
// way up, so a panicking request leaves no leaked cancel and no stale entry. A
// returned (non-panic) error is NOT a death — closure belongs to the sender — so
// it is swallowed for observability.
func (c *cell) safeReceive(env *message.Envelope) {
	reqCtx, reqCancel := c.requestCtx(env)
	c.armRequest(env.ID, reqCancel)
	defer func() {
		c.disarmRequest(env.ID)
		reqCancel()
	}()
	if err := c.impl.Receive(reqCtx, env); err != nil {
		_ = err
	}
}

// requestCtx derives the per-request ctx from c.ctx: a deadline when the
// envelope carries ExpiresAt (the request's own expiry, in Unix millis), else a
// plain cancel. Deriving from c.ctx is what makes cell death (c.cancel) and
// runtime teardown cascade into every live request.
func (c *cell) requestCtx(env *message.Envelope) (context.Context, context.CancelFunc) {
	if env.ExpiresAt != nil {
		return context.WithDeadline(c.ctx, time.UnixMilli(*env.ExpiresAt))
	}
	return context.WithCancel(c.ctx)
}

// armRequest records a live request's cancel under its id. Last-writer wins on a
// colliding id (an at-most-one-live-per-id invariant the serial mailbox already
// holds); the prior cancel is fired so no handle leaks.
func (c *cell) armRequest(id message.ID, cancel context.CancelFunc) {
	c.flightMu.Lock()
	if prev, ok := c.inflight[id]; ok {
		prev()
	}
	c.inflight[id] = cancel
	c.flightMu.Unlock()
}

// disarmRequest removes a request's entry (the table never outlives its
// requests). It does NOT fire the cancel — safeReceive's defer does that.
func (c *cell) disarmRequest(id message.ID) {
	c.flightMu.Lock()
	delete(c.inflight, id)
	c.flightMu.Unlock()
}

// cancelRequest implements embodiment: fire the in-flight reqCtx for id, off the
// cell goroutine. This is the request-scope of cancel(scope) — it interrupts
// exactly the one Receive holding the goroutine without queuing behind it (the
// thing to interrupt IS the goroutine's occupant). Idempotent and unknown-id
// safe: a request that already closed (or never existed) is a no-op, because the
// caller's closure owns the terminal — the cancel is a best-effort hint. It does
// NOT remove the entry; safeReceive's own defer disarms on the way out.
func (c *cell) cancelRequest(id message.ID) {
	c.flightMu.Lock()
	cancel := c.inflight[id]
	c.flightMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// initiateStop implements embodiment: the non-blocking, idempotent SIGNAL half of
// teardown — trigger death and return at once, WITHOUT joining c.done.
// It is stop()'s signal half; stop() = initiateStop() + join. Both share
// stopOnce, so calling either (or both, in either order) is safe — the body
// runs exactly once, and c.done is safe to read from multiple goroutines.
//
// It calls onExit IMMEDIATELY (mirroring port.die()'s existing onExit call) so
// a cascaded child is removed from r.embodiments/LiveIDs() the instant this is
// called, not lazily whenever its own goroutine notices ctx cancellation — a
// dying parent's cascade (removeIf) calls ONLY this, never stop(), because
// stop() joins the child's goroutine and a child's initiateStop synchronously
// re-enters onExit/removeIf (which takes r.mu) — calling it while the parent's
// own removeIf still held r.mu would deadlock (it doesn't: removeIf calls this
// AFTER releasing r.mu).
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

// stop closes the mailbox, cancels the cell ctx, and waits for the goroutine to
// exit. Safe to call multiple times. It MUST be called only from a DIFFERENT
// goroutine than the cell's own (it joins on c.done) — the death path never
// calls stop(); it calls initiateStop() (the signal-only half) instead.
func (c *cell) stop() {
	c.initiateStop()
	<-c.done
}
