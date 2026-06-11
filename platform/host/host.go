package host

import (
	"context"
	"sync"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// Host hosts business cells on a daemon. It owns an actorrt.Runtime (the cells)
// and, per hosted actor, a downHandler the link layer installs so a cell's death
// closes its own stream (the home port then reads EOF). No truth, no wire
// vocabulary — the cell's writer (the link's ipc.RemoteWriter) is injected at
// Install.
type Host struct {
	rt        *actorrt.Runtime
	deliverer actorrt.Deliverer

	mu   sync.Mutex
	down map[actor.ActorID]func(cause string)
}

// New constructs a host. It registers itself as the runtime's PresenceWatcher:
// when a hosted cell dies abnormally, OnDown fires the actor's downHandler
// (close its stream UP the link).
func New() *Host {
	rt, del := actorrt.New(actorrt.Config{})
	h := &Host{
		rt:        rt,
		deliverer: del,
		down:      map[actor.ActorID]func(string){},
	}
	rt.WatchPresence(h)
	return h
}

// Install spawns impl as a cell whose writer is the injected out-of-process pen
// (ipc.RemoteWriter over its link stream). downHandler is invoked when the cell
// dies — the link layer closes that actor's stream so the home port reads EOF.
func (h *Host) Install(id actor.ActorID, impl actorrt.Actor, downHandler func(cause string)) {
	h.mu.Lock()
	h.down[id] = downHandler
	h.mu.Unlock()
	h.rt.Spawn(id, impl)
}

// OnDown implements actorrt.PresenceWatcher. A hosted cell died abnormally — the
// daemon holds no truth, so it cannot write receiver_unavailable itself. It
// fires the actor's downHandler (close its stream UP the link); the home port
// reads EOF and the home's closure author#3 closes in-flight requests.
func (h *Host) OnDown(_ context.Context, id actor.ActorID, cause error) {
	h.mu.Lock()
	handler := h.down[id]
	h.mu.Unlock()
	if handler != nil {
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		handler(msg)
	}
}

// Dispatch routes an inbound envelope to the hosted cell's mailbox.
func (h *Host) Dispatch(target actor.ActorID, env *message.Envelope) error {
	_, err := h.deliverer.Deliver([]actor.ActorID{target}, env)
	return err
}

// CancelRequest cancels one in-flight request's reqCtx on a hosted cell — the
// request-scope of cancel(scope) the home reaches across the wire (KindCancel).
// It bypasses the mailbox: the runtime fires the cell's in-flight CancelFunc off
// the work goroutine, so the Receive currently occupying that goroutine is
// interrupted instead of queuing the cancel behind it. No-op if the actor is not
// hosted or the request already closed.
func (h *Host) CancelRequest(target actor.ActorID, requestID message.ID) {
	h.rt.CancelRequest(target, requestID)
}

// Stop tears down all hosted cells.
func (h *Host) Stop() { h.rt.StopAll() }

// Verify PresenceWatcher conformance at compile time.
var _ actorrt.PresenceWatcher = (*Host)(nil)
