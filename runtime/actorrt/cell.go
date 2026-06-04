package actorrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrMailboxFull is returned by Deliver when the actor's bounded mailbox is
// full. The caller MUST NOT block: a full request mailbox is rejected at the
// seam and the sender's caller-scoped closure (or the substrate presence-down edge)
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
	// control is the separate control lane: a bounded queue the runtime's
	// Controller enqueues Signals into, drained by the SAME cell goroutine but
	// PRIORITIZED ahead of work. Single-serialization is preserved (one
	// processor, one thing at a time), while control jumps the work backlog — so
	// reload/quota/stop reach the actor even with a deep inbox.
	control chan Signal

	// started is the substrate-stamped bind instant — the obs `uptime` fact's
	// authoritative source (uptime = now - started, derived by the consumer).
	// Only the substrate can produce it (Erlang /proc starttime model), so it is
	// stamped at Spawn, never self-reported by the actor.
	started time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// onDown publishes the obs presence DELETED edge (death) for this id. The
	// runtime wires it to its presence-watch fanout. Death is an obs push, not a
	// control signal and not truth — the watcher's reaction (receiver_unavailable)
	// is the part that becomes work/truth.
	onDown func(actor.ActorID, error)
	// onObs publishes an actor-produced obs snapshot (obs push/actor) to the
	// runtime's per-actor obs watchers. Invoked from the actor via
	// ActorContext.PublishObs. No watcher → no-op.
	onObs func(actor.ActorID, ObsKind, ObsValue)
	// logger surfaces control-lane edge observability (default dispositions).
	logger *slog.Logger
	// onExit is the pointer-identity-checked self-eviction hook the Runtime injects:
	// on ANY exit (clean or panic) the cell removes itself from the addressing
	// map IFF the map still points to this very instance. This is how death
	// makes an instance unaddressable WITHOUT the cell trying to stop/join
	// itself (a goroutine cannot join itself — that was the death deadlock).
	onExit func(actor.ActorID, presence)

	// done is closed when the cell goroutine has fully exited (after Stop).
	done chan struct{}

	stopOnce sync.Once
	// closed guards inbox-send so Deliver never enqueues into a torn-down cell.
	mu     sync.Mutex
	closed bool
}

// newCell constructs a cell. mailbox is the bounded inbox depth; onExit is the
// self-eviction hook; onDown publishes the death (presence DELETED) edge;
// started is the substrate-stamped bind instant (obs uptime).
func newCell(parent context.Context, id actor.ActorID, impl Actor, mailbox int, onDown func(actor.ActorID, error), onObs func(actor.ActorID, ObsKind, ObsValue), onExit func(actor.ActorID, presence), started time.Time, logger *slog.Logger) *cell {
	if mailbox <= 0 {
		mailbox = 64
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(parent)
	return &cell{
		id:      id,
		impl:    impl,
		inbox:   make(chan *message.Envelope, mailbox),
		control: make(chan Signal, mailbox),
		started: started,
		ctx:     ctx,
		cancel:  cancel,
		onDown:  onDown,
		onObs:   onObs,
		logger:  logger,
		onExit:  onExit,
		done:    make(chan struct{}),
	}
}

// Self implements ActorContext.
func (c *cell) Self() actor.ActorID { return c.id }

// startedAt implements presence: the substrate-stamped bind instant (obs uptime
// source). The substrate is the authority for it — the cell never self-reports.
func (c *cell) startedAt() time.Time { return c.started }

// observe implements presence: the obs PULL/actor route. It forwards the opaque
// kind to the actor IFF the actor opts in via the Observer hook, answered
// concurrently (out-of-band, not on the work goroutine) — so the impl must be
// non-perturbing. An actor that does not implement Observer is a no-op:
// ErrObsUnsupported.
func (c *cell) observe(ctx context.Context, kind ObsKind) (ObsValue, error) {
	// Lifecycle guard (same as Deliver/signal): do not run an obs hook on a cell
	// that has begun teardown — its Stop may be releasing the very state Observe
	// would read.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCellStopped
	}
	c.mu.Unlock()
	if obs, ok := c.impl.(Observer); ok {
		return obs.Observe(ctx, kind)
	}
	return nil, ErrObsUnsupported
}

// PublishObs implements ActorContext: the actor's obs PUSH/producer end. It
// hands an opaque snapshot to the runtime's per-actor obs fanout (no watcher →
// no-op). This is NOT a self-send and NOT truth — it publishes observable state,
// the substrate forwards it without interpreting.
func (c *cell) PublishObs(kind ObsKind, val ObsValue) {
	if c.onObs != nil {
		c.onObs(c.id, kind, val)
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

// signal is the substrate enqueue path into this cell's CONTROL lane — held only
// by the runtime's Controller, never exposed to the actor. Non-blocking: a full
// lane returns ErrMailboxFull, a stopped cell ErrCellStopped.
func (c *cell) signal(sig Signal) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCellStopped
	}
	c.mu.Unlock()

	select {
	case c.control <- sig:
		return nil
	default:
		return ErrMailboxFull
	}
}

// start spawns the cell goroutine. It calls Start (if implemented), then loops
// over the mailbox invoking Receive serially, then on teardown self-evicts,
// publishes the presence DELETED edge (on abnormal death), and calls Stop. A
// panic anywhere is recovered and surfaced as that death edge.
func (c *cell) start() {
	go func() {
		defer close(c.done)

		var deathCause error
		defer func() {
			if r := recover(); r != nil {
				deathCause = fmt.Errorf("actorrt: cell %s panicked: %v", c.id, r)
			}
			// (a) Make this instance unaddressable FIRST — pointer-identity
			// self-eviction. Never self-stop()/join: a goroutine cannot wait on
			// its own exit.
			if c.onExit != nil {
				c.onExit(c.id, c)
			}
			// (b) On abnormal death, PUBLISH the presence DELETED edge (obs push).
			// The runtime fans it out to presence watchers (e.g. the closure-doer
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
			// Control lane is PRIORITIZED: drain a pending control signal before
			// touching work, so reload/quota/stop preempt a work backlog.
			select {
			case sig := <-c.control:
				c.handleControl(sig)
				continue
			default:
			}
			select {
			case <-c.ctx.Done():
				return
			case sig := <-c.control:
				c.handleControl(sig)
			case env := <-c.inbox:
				c.safeReceive(env)
			}
		}
	}()
}

// handleControl dispatches one control signal on the cell goroutine (serially,
// never concurrent with Receive). If the actor implements Controllable it runs
// OnControl (a panic there propagates to the death path, like Receive). Else the
// runtime DEFAULT DISPOSITION applies: SignalStop self-cancels the cell (a clean
// exit, no death edge); other kinds are ignored (Unix default action for an
// uncaught signal). Forcible teardown of a wedged cell is the runtime's hosting
// power (Despawn), not this cooperative path.
func (c *cell) handleControl(sig Signal) {
	if ctrl, ok := c.impl.(Controllable); ok {
		ctrl.OnControl(c.ctx, sig)
		return
	}
	// No Controllable → runtime default disposition (Unix default action).
	if sig.Kind == SignalStop {
		c.logger.Info("actorrt.control.default_stop", "actor", c.id)
		c.cancel()
		return
	}
	c.logger.Info("actorrt.control.default_ignore", "actor", c.id, "kind", sig.Kind)
}

// safeReceive invokes impl.Receive. A panic propagates naturally up the cell
// goroutine stack to the deferred recover in start(), which converts it into a
// single presence-down publish (one death per cell, not per message) — safeReceive itself
// does not recover. A returned (non-panic) error is NOT a death — closure
// belongs to the sender — so it is swallowed for observability (the substrate
// never synthesises a terminal from a handler error; an actor needing
// observability emits it itself).
func (c *cell) safeReceive(env *message.Envelope) {
	if err := c.impl.Receive(c.ctx, env); err != nil {
		_ = err
	}
}

// stop closes the mailbox, cancels the cell ctx, and waits for the goroutine to
// exit. Safe to call multiple times. It MUST be called only from a DIFFERENT
// goroutine than the cell's own (it joins on c.done) — the death path never
// calls stop(); it self-evicts via onExit instead.
func (c *cell) stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
	})
	<-c.done
}
