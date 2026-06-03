package actorrt

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrMailboxFull is returned by Deliver when the actor's bounded mailbox is
// full. The caller MUST NOT block: a full request mailbox is rejected at the
// seam and the sender's caller-scoped closure (or the substrate death signal)
// collapses it. An event whose mailbox is full may be dropped (the log is truth).
var ErrMailboxFull = errors.New("actorrt: mailbox full")

// ErrCellStopped is returned by Deliver after the cell has begun teardown.
var ErrCellStopped = errors.New("actorrt: cell stopped")

// DeathSignal is what a Supervisor receives when a cell's actor goroutine
// terminates abnormally (panic in Receive/Start). It names the dead actor by
// its stable ActorID. Death is TERMINAL for that id — the substrate does no
// transparent same-id respawn, so there is no successor a stale death could be
// confused with and no per-instance generation is needed; a supervisor
// collapses the dead actor's in-flight requests by ActorID.
type DeathSignal struct {
	Actor actor.ActorID
	// Cause is the recovered panic value (or error) that killed the cell.
	Cause error
}

// Supervisor observes cell death. The runtime calls OnDeath exactly once per
// cell that dies abnormally. It is the seam where the substrate materialises
// receiver_unavailable for the dead actor's in-flight requests; the
// substrate never guesses "slow" — it only reports death it positively
// observed. The dead cell has ALREADY evicted itself from the runtime's
// addressing map before OnDeath runs, so a supervisor MUST NOT despawn it.
type Supervisor interface {
	OnDeath(ctx context.Context, sig DeathSignal)
}

// cell is one actor INSTANCE: its impl (private state), a single goroutine, and
// a bounded envelope mailbox. The mailbox carries ONLY envelopes — the
// substrate's isolation guarantee is that the sole way to affect an actor is to
// send it a message; there is no path to run caller-supplied code on the cell
// goroutine.
type cell struct {
	id    actor.ActorID
	impl  Actor
	inbox chan *message.Envelope

	ctx    context.Context
	cancel context.CancelFunc

	sup Supervisor
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
// self-eviction hook.
func newCell(parent context.Context, id actor.ActorID, impl Actor, mailbox int, sup Supervisor, onExit func(actor.ActorID, presence)) *cell {
	if mailbox <= 0 {
		mailbox = 64
	}
	ctx, cancel := context.WithCancel(parent)
	return &cell{
		id:     id,
		impl:   impl,
		inbox:  make(chan *message.Envelope, mailbox),
		ctx:    ctx,
		cancel: cancel,
		sup:    sup,
		onExit: onExit,
		done:   make(chan struct{}),
	}
}

// Self implements ActorContext.
func (c *cell) Self() actor.ActorID { return c.id }

// Deliver implements ActorContext and is also the substrate enqueue path. It
// never blocks: a full mailbox returns ErrMailboxFull, a stopped cell
// ErrCellStopped.
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
// signals the supervisor (on abnormal death), and calls Stop. A panic anywhere
// is recovered and surfaced as a DeathSignal.
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
			// (b) On abnormal death, hand the supervisor the closure obligation.
			// Guard it: a supervisor panic must not escape and crash the process
			// through the death path. Use a background-derived ctx (the cell ctx
			// may already be cancelled, but the terminal still needs writing).
			if deathCause != nil && c.sup != nil {
				func() {
					defer func() { _ = recover() }()
					c.sup.OnDeath(context.Background(), DeathSignal{
						Actor: c.id,
						Cause: deathCause,
					})
				}()
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

// safeReceive invokes impl.Receive. A panic propagates naturally up the cell
// goroutine stack to the deferred recover in start(), which converts it into a
// single DeathSignal (one death per cell, not per message) — safeReceive itself
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
