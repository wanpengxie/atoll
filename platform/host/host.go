package host

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

// DeathFunc reports a hosted cell's death UP to the home so the home can
// materialise receiver_unavailable for its in-flight requests. homelink injects
// a computebus DeathFrame sender.
type DeathFunc func(actor.ActorID, string)

// Host hosts business cells (tool/agent actors) on an attached compute. It maps
// a DispatchFrame from the home to the cell mailbox, and each cell's output
// flows UP via an UplinkWriter (NO local truth -- daemon is attached compute).
//
// Host registers itself as a PresenceWatcher on the actorrt.Runtime: when a
// hosted cell dies the substrate publishes the death edge, and Host propagates
// it UP via DeathFunc (the compute side has no truth to write).
type Host struct {
	rt        *Runtime
	deliverer actorrt.Deliverer
	emit      EmitFunc
	sendDeath DeathFunc
}

// Runtime is the actorrt.Runtime exposed for callers that need to Spawn cells
// directly (e.g. daemon.Run installing actors). It is deliberately a type alias
// so Host does not re-export the full actorrt surface.
type Runtime = actorrt.Runtime

// New constructs a host. emit is the homelink-injected uplink to the home
// harness; sendDeath propagates cell death UP the wire.
func New(emit EmitFunc, sendDeath DeathFunc) *Host {
	rt, del, _ := actorrt.New(actorrt.Config{})
	h := &Host{
		rt:        rt,
		deliverer: del,
		emit:      emit,
		sendDeath: sendDeath,
	}
	rt.WatchPresence(h)
	return h
}

// OnDown implements actorrt.PresenceWatcher. A hosted cell died abnormally --
// the compute holds no truth, so it cannot write receiver_unavailable itself.
// It reports the death UP via DeathFunc (FrameDeath); the home's fleet calls
// the closure author#3 to close in-flight requests.
func (h *Host) OnDown(_ context.Context, id actor.ActorID, cause error) {
	if h.sendDeath != nil {
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		h.sendDeath(id, msg)
	}
}

// Install spawns an actorrt.Actor as a cell on this compute, wiring its writer
// to the uplink so behavior.Respond / behavior.EmitEvent flow to the home
// harness. Returns the UplinkWriter so the caller can pass it to the actor
// constructor if needed.
func (h *Host) Install(id actor.ActorID, impl actorrt.Actor) *UplinkWriter {
	w := NewUplinkWriter(id, h.emit)
	h.rt.Spawn(id, impl)
	return w
}

// Dispatch routes an inbound DispatchFrame to the hosted cell's mailbox.
func (h *Host) Dispatch(frame computebus.DispatchFrame) error {
	_, err := h.deliverer.Deliver([]actor.ActorID{frame.Target}, frame.Envelope)
	return err
}

// Runtime exposes the underlying actorrt.Runtime for direct Spawn/Despawn.
func (h *Host) Rt() *Runtime { return h.rt }

// Stop tears down all hosted cells.
func (h *Host) Stop() { h.rt.StopAll() }

// Verify PresenceWatcher conformance at compile time.
var _ actorrt.PresenceWatcher = (*Host)(nil)
