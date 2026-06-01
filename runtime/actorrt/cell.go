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
// full. The caller MUST NOT block: per actor-runtime-redesign.md §7.2 a
// full request mailbox is rejected at the seam and the sender's
// caller-scoped closure (or the substrate death signal) collapses it. An
// event whose mailbox is full may be dropped (the log is truth).
var ErrMailboxFull = errors.New("actorrt: mailbox full")

// ErrCellStopped is returned by Deliver after the cell has begun teardown.
var ErrCellStopped = errors.New("actorrt: cell stopped")

// ErrNoCell is returned by Runtime.Do/Ask when no cell is hosted for the
// target id (the substrate cannot run a closure on an actor it does not host).
var ErrNoCell = errors.New("actorrt: no cell for actor")

// job is one mailbox entry. Exactly one of {env, fn, ask} is set:
//   - env: an envelope to Receive (the normal message path)
//   - fn:  a closure to run on the cell goroutine, fire-and-forget (the
//     out-of-band signal path — e.g. a device lifecycle frame — folded onto
//     the actor's own goroutine so it touches state serially, WITHOUT
//     encoding transport signals as protocol envelopes)
//   - ask: a closure run on the cell goroutine whose error is returned
//     SYNCHRONOUSLY to the caller via result (the device-callback ack path:
//     bad/duplicate/expired frames must be rejected with a permanent/retryable
//     error the transit layer hands back to the device — dismantle §2.5-A a).
type job struct {
	env    *message.Envelope
	fn     func()
	ask    func(context.Context) error
	result chan error
}

// DeathSignal is what a Supervisor receives when a cell's actor goroutine
// terminates abnormally (panic in Receive/Start/Stop). It carries enough to
// let the supervisor materialise a receiver_unavailable terminal for every
// in-flight request addressed to the dead actor (the substrate's only
// closure obligation — actor-runtime-redesign.md §5).
type DeathSignal struct {
	Actor actor.ActorID
	// Cause is the recovered panic value (or error) that killed the cell.
	Cause error
}

// Supervisor observes cell death. The runtime calls OnDeath exactly once
// per cell that dies abnormally. It is the seam where the substrate
// materialises receiver_unavailable for the dead actor's in-flight
// requests; the substrate never guesses "slow" — it only reports death it
// positively observed.
type Supervisor interface {
	OnDeath(ctx context.Context, sig DeathSignal)
}

// cell is one actor instance: its impl (private state), a single goroutine,
// and a bounded inbox channel (the mailbox). It is the Go physical form of
// actor-runtime-redesign.md §1.4.
type cell struct {
	id    actor.ActorID
	impl  Actor
	inbox chan job

	ctx    context.Context
	cancel context.CancelFunc

	sup Supervisor

	// done is closed when the cell goroutine has fully exited (after Stop).
	done chan struct{}

	stopOnce sync.Once
	// closed guards inbox-close so Deliver never sends on a closed channel.
	mu     sync.Mutex
	closed bool
}

// newCell constructs a cell. mailbox is the bounded inbox depth.
func newCell(parent context.Context, id actor.ActorID, impl Actor, mailbox int, sup Supervisor) *cell {
	if mailbox <= 0 {
		mailbox = 64
	}
	ctx, cancel := context.WithCancel(parent)
	return &cell{
		id:     id,
		impl:   impl,
		inbox:  make(chan job, mailbox),
		ctx:    ctx,
		cancel: cancel,
		sup:    sup,
		done:   make(chan struct{}),
	}
}

// Self implements ActorContext.
func (c *cell) Self() actor.ActorID { return c.id }

// Deliver implements ActorContext and is also the substrate enqueue path.
// It never blocks: a full mailbox returns ErrMailboxFull, a stopped cell
// ErrCellStopped.
func (c *cell) Deliver(env *message.Envelope) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCellStopped
	}
	c.mu.Unlock()

	select {
	case c.inbox <- job{env: env}:
		return nil
	default:
		return ErrMailboxFull
	}
}

// start spawns the cell goroutine. It calls Start (if implemented), then
// loops over the mailbox invoking Receive serially, then drains and calls
// Stop on teardown. A panic anywhere is recovered and surfaced as a
// DeathSignal to the supervisor.
func (c *cell) start() {
	go func() {
		defer close(c.done)

		var deathCause error
		defer func() {
			if r := recover(); r != nil {
				deathCause = fmt.Errorf("actorrt: cell %s panicked: %v", c.id, r)
			}
			if deathCause != nil && c.sup != nil {
				// Use a background-derived ctx: the cell ctx may already be
				// cancelled, but the supervisor still needs to write the
				// death terminal.
				c.sup.OnDeath(context.Background(), DeathSignal{Actor: c.id, Cause: deathCause})
			}
			// Best-effort resource release.
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
			case j := <-c.inbox:
				switch {
				case j.ask != nil:
					j.result <- c.safeAsk(j.ask)
				case j.fn != nil:
					j.fn() // runs on the cell goroutine; a panic propagates to the outer recover → DeathSignal
				default:
					c.safeReceive(j)
				}
			}
		}
	}()
}

// post enqueues a closure to run on the cell goroutine. It is the out-of-band
// signal path (e.g. a device lifecycle frame folded onto the actor) so the
// actor touches its state serially. Never blocks: full mailbox → ErrMailboxFull,
// stopped cell → ErrCellStopped.
func (c *cell) post(fn func()) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCellStopped
	}
	c.mu.Unlock()
	select {
	case c.inbox <- job{fn: fn}:
		return nil
	default:
		return ErrMailboxFull
	}
}

// ask enqueues a closure to run on the cell goroutine and blocks until it
// returns, surfacing its error synchronously. This is the device-callback ack
// path (dismantle §2.5-A a): the transit layer needs a permanent/retryable/ok
// verdict computed serially on the actor's own goroutine. Never blocks on a
// full mailbox (→ ErrMailboxFull); if the cell tears down while the ask is
// queued, returns ErrCellStopped.
func (c *cell) ask(fn func(context.Context) error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCellStopped
	}
	c.mu.Unlock()

	res := make(chan error, 1)
	select {
	case c.inbox <- job{ask: fn, result: res}:
	default:
		return ErrMailboxFull
	}
	select {
	case err := <-res:
		return err
	case <-c.ctx.Done():
		return ErrCellStopped
	}
}

// safeAsk runs an ask closure with panic recovery, converting a panic into a
// returned error. Unlike Receive, an ask panic does NOT kill the cell: ask is
// a controlled synchronous entry whose caller owns the verdict, so the panic
// is reported back rather than turned into a DeathSignal.
func (c *cell) safeAsk(fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("actorrt: cell %s ask panicked: %v", c.id, r)
		}
	}()
	return fn(c.ctx)
}

// safeReceive invokes impl.Receive with panic recovery. A panic is
// re-raised as a goroutine-level panic so the cell's outer recover converts
// it into a DeathSignal (one death per cell, not per message).
func (c *cell) safeReceive(j job) {
	if err := c.impl.Receive(c.ctx, j.env); err != nil {
		// A returned (non-panic) error is not a death — closure belongs to
		// the sender. We swallow it here; callers needing observability can
		// have the actor itself emit. (Substrate never synthesises terminal
		// from a mere handler error.)
		_ = err
	}
}

// stop closes the mailbox, cancels the cell ctx, and waits for the
// goroutine to exit. Safe to call multiple times.
func (c *cell) stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
	})
	<-c.done
}
