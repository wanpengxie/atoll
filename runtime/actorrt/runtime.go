package actorrt

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// presence is one live actor's substrate-side presence, regardless of where
// the actor's code runs. Deliver enqueues an envelope into the actor's mailbox
// (never blocking: ErrMailboxFull / ErrCellStopped); stop tears it down. Two
// backends implement it, by transport distance:
//   - cell  — in-process actor   (mailbox = Go channel)
//   - port  — out-of-process actor (mailbox = byte-stream connection)
//
// Both report the same Outcome on Deliver and the same DeathSignal on death.
type presence interface {
	Deliver(env *message.Envelope) error
	stop()
}

// Runtime owns the live presences for one channel and is the addressing seam.
// Identity is single-level: a stable ActorID names at most one live presence at
// a time. Spawn/Attach replaces; death is terminal (no transparent same-id
// respawn). Death and replacement are checked by POINTER IDENTITY (the map
// entry still points to this very presence), so a dying predecessor can never
// evict its successor — no per-instance generation is needed.
type Runtime struct {
	parent context.Context
	sup    Supervisor

	mu        sync.RWMutex
	presences map[actor.ActorID]presence
	mailbox   int
}

// Config configures a Runtime.
type Config struct {
	// Parent is the context all presences derive from; cancelling it tears the
	// whole channel down.
	Parent context.Context
	// Supervisor receives death signals from presences; may be nil.
	Supervisor Supervisor
	// Mailbox is the default bounded mailbox depth (<=0 → 64).
	Mailbox int
}

// Deliverer is the privileged capability to enqueue an envelope into a hosted
// cell's mailbox. It is the ONLY way to feed a mailbox, and New hands it out
// EXACTLY ONCE — it is deliberately NOT a method on the broadly-shared *Runtime
// handle. The composition root routes it to the single legitimate feeder: the
// post-harness fanout (every envelope it enqueues has already committed to
// truth) and that fanout's wire-dispatch arm. So code that merely holds a
// *Runtime (to Spawn / address / query) CANNOT inject into a mailbox and thus
// cannot bypass the harness — the mailbox is the harness pipeline's private
// egress, structurally, not by convention.
type Deliverer interface {
	Deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error)
}

// New constructs a Runtime and its sole Deliverer. The *Runtime is the
// broadly-shareable management/addressing handle (Spawn/Attach/Despawn/Has/
// StopAll); the Deliverer is the confined enqueue capability — give it ONLY to
// the post-harness fanout.
func New(cfg Config) (*Runtime, Deliverer) {
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	mb := cfg.Mailbox
	if mb <= 0 {
		mb = 64
	}
	r := &Runtime{
		parent:    parent,
		sup:       cfg.Supervisor,
		presences: make(map[actor.ActorID]presence),
		mailbox:   mb,
	}
	return r, deliverer{r}
}

// deliverer is the only holder-confined implementation of Deliverer; it wraps
// the runtime and calls its unexported enqueue (unreachable from other packages
// via the shared *Runtime).
type deliverer struct{ r *Runtime }

func (d deliverer) Deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error) {
	return d.r.deliver(audience, env)
}

// Outcome is the per-audience truth Deliver reports about its own action — the
// substrate knows whether it hosts an addressed actor and must not misreport it.
type Outcome int

const (
	// Delivered: the envelope was enqueued into the actor's mailbox.
	Delivered Outcome = iota
	// NotHosted: this runtime hosts no presence for the actor (legitimately —
	// the audience may include system/user/remote-elsewhere actors). The seam
	// can fast-fail receiver_unavailable instead of waiting for a timeout.
	NotHosted
	// MailboxFull: a presence is hosted but its bounded mailbox is full.
	MailboxFull
	// Stopped: a presence is hosted but tearing down.
	Stopped
)

// DeliverResult is the structured, per-audience truth of a Deliver call: the
// Outcome for each addressed actor.
type DeliverResult struct {
	Per map[actor.ActorID]Outcome
}

// Spawn creates and starts an IN-PROCESS cell for id. If a presence already
// exists for id it is stopped and replaced (one actor, one owner).
func (r *Runtime) Spawn(id actor.ActorID, impl Actor) {
	r.mu.Lock()
	c := newCell(r.parent, id, impl, r.mailbox, r.sup, r.removeIf)
	old, existed := r.presences[id]
	r.presences[id] = c
	r.mu.Unlock()
	// stop()/start() run OUTSIDE the lock: stop() joins the old goroutine,
	// which may itself call back into the runtime.
	if existed {
		old.stop()
	}
	c.start()
}

// Attach binds an out-of-process actor that CONNECTED IN over conn, registering
// it as a `port` presence. The substrate does not spawn the remote (it connects
// in); Attach performs the handshake, resolves
// the connection's credential to an ActorID via resolve, relays the remote's
// emits through emit, and returns the bound id. If a presence already exists for
// the resolved id it is stopped and replaced.
//
// No ctx: the bound port's lifetime is the runtime's (r.parent), NOT this call —
// a per-call ctx would wrongly scope the port to the Attach invocation. Attach
// itself does no cancelable wait (the handshake is bounded by conn deadlines).
func (r *Runtime) Attach(conn io.ReadWriteCloser, emit EmitSink, resolve ResolveFunc) (actor.ActorID, error) {
	p, err := newPort(r.parent, conn, emit, resolve, r.sup, r.removeIf)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	old, existed := r.presences[p.id]
	r.presences[p.id] = p
	r.mu.Unlock()
	if existed {
		old.stop()
	}
	p.start()
	return p.id, nil
}

// removeIf is the pointer-identity-checked self-eviction hook handed to each
// presence. It deletes the map entry IFF it still points to this very instance —
// so a late-dying predecessor (already replaced) cannot evict its successor.
func (r *Runtime) removeIf(id actor.ActorID, self presence) {
	r.mu.Lock()
	if cur, ok := r.presences[id]; ok && cur == self {
		delete(r.presences, id)
	}
	r.mu.Unlock()
}

// Despawn stops and removes the current presence for id (no-op if absent). This
// is the external deregister path; callers MUST ensure in-flight requests
// addressed to id are collapsed (substrate writes receiver_unavailable) before
// despawn.
func (r *Runtime) Despawn(id actor.ActorID) {
	r.mu.Lock()
	p, ok := r.presences[id]
	if ok {
		delete(r.presences, id)
	}
	r.mu.Unlock()
	if ok {
		p.stop()
	}
}

// deliver routes env to every audience presence hosted by this Runtime by
// enqueueing into each mailbox, returning the per-audience Outcome. An audience
// member with no local presence is reported NotHosted (not silently skipped) —
// the substrate reports truthfully what it did so the seam can fast-fail. error
// is reserved for a true exception (nil envelope), not for delivery conditions.
//
// Unexported: the enqueue is reachable ONLY through the Deliverer capability New
// hands out (a *Runtime holder cannot call it), so the mailbox stays the harness
// pipeline's private egress.
//
// No ctx: the enqueue is a non-blocking mailbox post (cell.Deliver never blocks
// — a full mailbox returns MailboxFull at once), so there is no cancelable wait
// for a ctx to act on. A per-call ctx would be pure decoration.
func (r *Runtime) deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error) {
	if env == nil {
		return DeliverResult{}, errors.New("actorrt: deliver nil envelope")
	}
	res := DeliverResult{Per: make(map[actor.ActorID]Outcome, len(audience))}
	r.mu.RLock()
	matched := make(map[actor.ActorID]presence, len(audience))
	for _, id := range audience {
		if p, ok := r.presences[id]; ok {
			matched[id] = p
		} else {
			res.Per[id] = NotHosted
		}
	}
	r.mu.RUnlock()

	for id, p := range matched {
		switch err := p.Deliver(env); err {
		case nil:
			res.Per[id] = Delivered
		case ErrMailboxFull:
			res.Per[id] = MailboxFull
		case ErrCellStopped:
			res.Per[id] = Stopped
		default:
			res.Per[id] = Stopped
		}
	}
	return res, nil
}

// StopAll stops every presence. Used at channel teardown.
func (r *Runtime) StopAll() {
	r.mu.Lock()
	ps := make([]presence, 0, len(r.presences))
	for _, p := range r.presences {
		ps = append(ps, p)
	}
	r.presences = make(map[actor.ActorID]presence)
	r.mu.Unlock()
	for _, p := range ps {
		p.stop()
	}
}

// Has reports whether a presence is hosted for id.
func (r *Runtime) Has(id actor.ActorID) bool {
	r.mu.RLock()
	_, ok := r.presences[id]
	r.mu.RUnlock()
	return ok
}
