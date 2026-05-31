package scheduler

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// HandlerFn processes one envelope addressed to actorID.
type HandlerFn func(ctx context.Context, actorID actor.ActorID, env *message.Envelope) error

// Deliverer dispatches an envelope to one or more actor handlers.
//
// The supervisor / adapter manager registers a handler per active actor.
// scheduler.Deliver iterates the envelope.Audience (resolved by trigger
// gateway in L1 §5.1) and invokes each handler.
//
// This package keeps no per-actor state — that lives in supervisor
// (lifecycle) + adapter manager. Deliverer is the routing seam.
type Deliverer struct {
	mu       sync.RWMutex
	handlers map[actor.ActorID]HandlerFn
}

// NewDeliverer returns an empty Deliverer.
func NewDeliverer() *Deliverer { return &Deliverer{handlers: make(map[actor.ActorID]HandlerFn)} }

// Register adds a handler. Replaces an existing handler for the same id.
func (d *Deliverer) Register(actorID actor.ActorID, fn HandlerFn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if fn == nil {
		delete(d.handlers, actorID)
		return
	}
	d.handlers[actorID] = fn
}

// Deliver routes the envelope to every registered audience actor. Errors
// from individual handlers are collected and returned joined.
func (d *Deliverer) Deliver(ctx context.Context, audience []actor.ActorID, env *message.Envelope) error {
	if env == nil {
		return errors.New("scheduler: deliver nil envelope")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	var errs []error
	for _, id := range audience {
		fn, ok := d.handlers[id]
		if !ok {
			// No local handler for an audience member is not a delivery
			// error: an envelope's audience legitimately includes actors
			// this deliverer does not host (system / user / remote /
			// collapsed facades). Deliver to the handlers present; a
			// request that reaches no handler is still closed by the
			// §6.4 long-pending fallback (expires_at), so skipping here
			// never loses closure — and it avoids spurious dispatch
			// errors for system/user observational audiences.
			continue
		}
		if err := fn(ctx, id, env); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
