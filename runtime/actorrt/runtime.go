package actorrt

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Runtime owns the live cells for one channel and is the addressing seam
// that replaces runtime/scheduler.Deliverer. Where Deliverer held a map of
// stateless HandlerFn invoked lock-free and concurrently, Runtime holds a
// map of cells, each an object with its own goroutine and mailbox; Deliver
// enqueues into the addressed cell's mailbox (never invokes the actor
// inline) so the actor processes serially.
type Runtime struct {
	parent context.Context
	sup    Supervisor

	mu    sync.RWMutex
	cells map[actor.ActorID]*cell
	// mailbox is the default bounded mailbox depth for newly spawned cells.
	mailbox int
}

// Config configures a Runtime.
type Config struct {
	// Parent is the context all cells derive from; cancelling it tears the
	// whole channel down.
	Parent context.Context
	// Supervisor receives death signals from cells; may be nil (deaths are
	// then only logged via the cell's resource release).
	Supervisor Supervisor
	// Mailbox is the default bounded mailbox depth (<=0 → 64).
	Mailbox int
}

// New constructs a Runtime.
func New(cfg Config) *Runtime {
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	mb := cfg.Mailbox
	if mb <= 0 {
		mb = 64
	}
	return &Runtime{
		parent:  parent,
		sup:     cfg.Supervisor,
		cells:   make(map[actor.ActorID]*cell),
		mailbox: mb,
	}
}

// Spawn creates and starts a cell for id with the given actor impl. If a
// cell already exists for id it is stopped and replaced (the new world's
// equivalent of Deliverer.Register replacing a handler), under the
// single-critical-section discipline of INVARIANT-5 (one actor, one owner).
func (r *Runtime) Spawn(id actor.ActorID, impl Actor) {
	c := newCell(r.parent, id, impl, r.mailbox, r.sup)
	// Replace under ONE critical section: the map swap is atomic, so no
	// concurrent Spawn can observe id absent and create a rival cell. The old
	// code released the lock around existing.stop() and re-acquired it — that
	// window let another Spawn(id)/Despawn slip a cell in, which this Spawn
	// then overwrote without stopping, orphaning a live goroutine (TOCTOU).
	// stop()/start() run OUTSIDE the lock because stop() blocks on the cell
	// goroutine, which may itself call back into the runtime (Deliver/Post).
	r.mu.Lock()
	old, existed := r.cells[id]
	r.cells[id] = c
	r.mu.Unlock()
	if existed {
		old.stop()
	}
	c.start()
}

// Despawn stops and removes the cell for id (no-op if absent). This is the
// deregister path: callers MUST ensure in-flight requests addressed to id
// are collapsed (substrate writes receiver_unavailable) before or as part
// of despawn — actor-runtime-construction-spec.md §3.4 reconciler collapse.
func (r *Runtime) Despawn(id actor.ActorID) {
	r.mu.Lock()
	c, ok := r.cells[id]
	if ok {
		delete(r.cells, id)
	}
	r.mu.Unlock()
	if ok {
		c.stop()
	}
}

// Deliver routes env to every audience cell hosted by this Runtime by
// enqueueing into each cell's mailbox. An audience member with no local
// cell is skipped (the envelope's audience legitimately includes actors
// this runtime does not host — system/user/remote). Unlike the old
// Deliverer, skipping here does NOT rely on any global long-pending
// fallback: closure is the sender's caller-scoped responsibility.
func (r *Runtime) Deliver(ctx context.Context, audience []actor.ActorID, env *message.Envelope) error {
	if env == nil {
		return errors.New("actorrt: deliver nil envelope")
	}
	r.mu.RLock()
	matched := make([]*cell, 0, len(audience))
	for _, id := range audience {
		if c, ok := r.cells[id]; ok {
			matched = append(matched, c)
		}
	}
	r.mu.RUnlock()

	var errs []error
	for _, c := range matched {
		if err := c.Deliver(env); err != nil {
			// Mailbox-full / stopped is a substrate-level delivery condition,
			// not a handler error. We surface it joined so the seam can
			// translate a full request mailbox into an immediate
			// receiver_unavailable for the sender (risk §7.2).
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Post folds a closure onto the cell goroutine for id, so an out-of-band
// signal (e.g. a device lifecycle frame) mutates the actor's state serially
// rather than racing Receive. Returns false if no cell is hosted for id (the
// caller may then fall back to a direct call). The closure MUST only touch
// that actor's own state.
func (r *Runtime) Post(id actor.ActorID, fn func()) bool {
	r.mu.RLock()
	c, ok := r.cells[id]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	_ = c.post(fn)
	return true
}

// Do runs fn on the cell goroutine for id and returns its error synchronously.
// It is the device-callback ack seam (dismantle §2.5-A a): a callback frame is
// validated and applied on the actor's own goroutine (so it serialises with
// Receive), and the permanent/retryable/ok verdict is handed back to the
// transit layer. Returns ErrNoCell if id is not hosted here, ErrMailboxFull if
// the cell's mailbox is full, ErrCellStopped if it is tearing down.
//
// Deliberately minimal: no typed result, no cross-actor chaining. Do is for the
// substrate edge (transit) calling INTO an actor, never for actor-to-actor
// calls (that is behavior.Call over envelopes), and the caller MUST NOT be the
// target cell's own goroutine (self-ask deadlocks).
func (r *Runtime) Do(id actor.ActorID, fn func(context.Context) error) error {
	r.mu.RLock()
	c, ok := r.cells[id]
	r.mu.RUnlock()
	if !ok {
		return ErrNoCell
	}
	return c.ask(fn)
}

// Ask is an alias for Do, named for the gen_server:call analogy.
func (r *Runtime) Ask(id actor.ActorID, fn func(context.Context) error) error {
	return r.Do(id, fn)
}

// StopAll stops every cell. Used at channel teardown.
func (r *Runtime) StopAll() {
	r.mu.Lock()
	cells := make([]*cell, 0, len(r.cells))
	for _, c := range r.cells {
		cells = append(cells, c)
	}
	r.cells = make(map[actor.ActorID]*cell)
	r.mu.Unlock()
	for _, c := range cells {
		c.stop()
	}
}

// Has reports whether a cell is hosted for id.
func (r *Runtime) Has(id actor.ActorID) bool {
	r.mu.RLock()
	_, ok := r.cells[id]
	r.mu.RUnlock()
	return ok
}
