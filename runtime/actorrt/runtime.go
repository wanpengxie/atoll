package actorrt

import (
	"context"
	"errors"

	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Runtime owns the live cells for one channel and is the addressing seam. Its
// identity model is TWO-LEVEL: a stable ActorID names a sequence of distinct
// cell instances over time (Spawn replaces), and each instance carries an
// incarnation (its generation). The map keys by ActorID and always resolves to
// the CURRENT incarnation; death and replacement are incarnation-checked so a
// dying predecessor can never evict its successor.
type Runtime struct {
	parent context.Context
	sup    Supervisor

	mu    sync.RWMutex
	cells map[actor.ActorID]*cell
	// gen tracks the next incarnation to mint per ActorID (monotonic; never
	// reused, so a death signal unambiguously names one instance).
	gen map[actor.ActorID]uint64
	// mailbox is the default bounded mailbox depth for newly spawned cells.
	mailbox int
}

// Config configures a Runtime.
type Config struct {
	// Parent is the context all cells derive from; cancelling it tears the
	// whole channel down.
	Parent context.Context
	// Supervisor receives death signals from cells; may be nil.
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
		gen:     make(map[actor.ActorID]uint64),
		mailbox: mb,
	}
}

// Outcome is the per-audience truth Deliver reports about its own action — the
// substrate knows whether it hosts an addressed actor and must not misreport it.
type Outcome int

const (
	// Delivered: the envelope was enqueued into the current incarnation's mailbox.
	Delivered Outcome = iota
	// NotHosted: this runtime hosts no cell for the actor (legitimately — the
	// audience may include system/user/remote actors). The seam can fast-fail
	// receiver_unavailable instead of waiting for a timeout.
	NotHosted
	// MailboxFull: a cell is hosted but its bounded mailbox is full.
	MailboxFull
	// Stopped: a cell is hosted but tearing down.
	Stopped
)

// AudienceOutcome is the result for one audience member.
type AudienceOutcome struct {
	Outcome Outcome
	// Incarnation is the generation the envelope was delivered to (set only
	// when Outcome==Delivered).
	Incarnation uint64
}

// DeliverResult is the structured, per-audience truth of a Deliver call.
type DeliverResult struct {
	Per map[actor.ActorID]AudienceOutcome
}

// Spawn creates and starts a cell for id, minting a fresh incarnation. If a
// cell already exists for id it is stopped and replaced (one actor, one owner).
func (r *Runtime) Spawn(id actor.ActorID, impl Actor) {
	r.mu.Lock()
	r.gen[id]++
	inc := r.gen[id]
	c := newCell(r.parent, id, inc, impl, r.mailbox, r.sup, r.removeIf)
	old, existed := r.cells[id]
	r.cells[id] = c
	r.mu.Unlock()
	// stop()/start() run OUTSIDE the lock: stop() joins the old cell goroutine,
	// which may itself call back into the runtime.
	if existed {
		old.stop()
	}
	c.start()
}

// removeIf is the incarnation-checked self-eviction hook handed to each cell.
// It deletes the map entry IFF it still points to this very instance — so a
// late-dying predecessor (already replaced by Spawn) cannot evict its successor.
func (r *Runtime) removeIf(c *cell) {
	r.mu.Lock()
	if cur, ok := r.cells[c.id]; ok && cur == c {
		delete(r.cells, c.id)
	}
	r.mu.Unlock()
}

// Despawn stops and removes the current cell for id (no-op if absent). This is
// the external deregister path; callers MUST ensure in-flight requests addressed
// to id are collapsed (substrate writes receiver_unavailable) before despawn.
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

// Deliver routes env to every audience cell hosted by this Runtime by enqueueing
// into each cell's mailbox, returning the per-audience Outcome. An audience
// member with no local cell is reported NotHosted (not silently skipped) — the
// substrate reports truthfully what it did so the seam can fast-fail. error is
// reserved for a true exception (nil envelope), not for delivery conditions.
func (r *Runtime) Deliver(ctx context.Context, audience []actor.ActorID, env *message.Envelope) (DeliverResult, error) {
	if env == nil {
		return DeliverResult{}, errors.New("actorrt: deliver nil envelope")
	}
	res := DeliverResult{Per: make(map[actor.ActorID]AudienceOutcome, len(audience))}
	r.mu.RLock()
	matched := make(map[actor.ActorID]*cell, len(audience))
	for _, id := range audience {
		if c, ok := r.cells[id]; ok {
			matched[id] = c
		} else {
			res.Per[id] = AudienceOutcome{Outcome: NotHosted}
		}
	}
	r.mu.RUnlock()

	for id, c := range matched {
		switch err := c.Deliver(env); err {
		case nil:
			res.Per[id] = AudienceOutcome{Outcome: Delivered, Incarnation: c.incarnation}
		case ErrMailboxFull:
			res.Per[id] = AudienceOutcome{Outcome: MailboxFull, Incarnation: c.incarnation}
		case ErrCellStopped:
			res.Per[id] = AudienceOutcome{Outcome: Stopped, Incarnation: c.incarnation}
		default:
			res.Per[id] = AudienceOutcome{Outcome: Stopped, Incarnation: c.incarnation}
		}
	}
	return res, nil
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
