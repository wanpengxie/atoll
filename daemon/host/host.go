// Package host runs business actor cells on an attached compute (v2). See doc.go.
package host

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/agentactor"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// DeathFunc reports a hosted cell's death UP to the home so the home can
// materialise receiver_unavailable for its in-flight requests. homelink injects
// a computebus DeathFrame sender.
type DeathFunc func(actor.ActorID, string)

// Host hosts business cells (tool/agent actors) on a compute. It maps a
// DispatchFrame from the home to the cell mailbox, and each cell's output flows
// UP via an UplinkChain (NO local truth — daemon is attached compute). Host is
// the actorrt.Supervisor for its cells: a compute cell death is observed here
// and propagated UP via DeathFunc (the compute side has no truth to write).
type Host struct {
	cells     *actorrt.Runtime
	emit      EmitFunc
	sendDeath DeathFunc
	// callbacks routes a device-transit callback to the owning adapter cell.
	callbacks map[actor.ActorID]adapterhost.CallbackTarget
}

// New constructs a host. emit is the homelink-injected uplink to the home
// harness; sendDeath propagates cell death UP the wire. Host installs itself as
// the runtime supervisor so it observes every abnormal cell termination.
func New(emit EmitFunc, sendDeath DeathFunc) *Host {
	h := &Host{emit: emit, sendDeath: sendDeath, callbacks: map[actor.ActorID]adapterhost.CallbackTarget{}}
	h.cells = actorrt.New(actorrt.Config{Supervisor: h})
	return h
}

// OnDeath implements actorrt.Supervisor. A hosted cell died abnormally — the
// compute holds no truth, so it cannot write the receiver_unavailable terminal
// itself. It reports the death UP via DeathFunc (FrameDeath); the home's fleet
// calls channelhost.MaterialiseComputeDeath to close the in-flight requests. The
// dead cell is then dropped locally.
func (h *Host) OnDeath(ctx context.Context, sig actorrt.DeathSignal) {
	if h.sendDeath != nil {
		cause := ""
		if sig.Cause != nil {
			cause = sig.Cause.Error()
		}
		h.sendDeath(sig.Actor, cause)
	}
	h.cells.Despawn(sig.Actor)
}

// InstallAdapter installs a Module as an adapterActor cell, wiring its chain to
// the uplink so Respond/EmitEvent flow to the home harness. base supplies the
// non-chain deps (registry/lookup/typeReg are home-side seams the homelink
// proxies; clock/obs are local).
func (h *Host) InstallAdapter(ctx context.Context, mod behavior.Module, base adapterhost.InstallDeps) (actor.ActorID, error) {
	decl := mod.Declares()
	deps := base
	deps.Chain = NewUplinkChain(decl.ActorID, h.emit)
	res, err := adapterhost.Install(ctx, mod, deps)
	if err != nil {
		return "", err
	}
	h.cells.Spawn(res.ActorID, res.Actor)
	if cb, ok := res.Actor.(adapterhost.CallbackTarget); ok {
		h.callbacks[res.ActorID] = cb // device callbacks route here
	}
	return res.ActorID, nil
}

// DeliverCallbackFrame routes a device-transit callback to the owning adapter
// cell, applied ON the cell goroutine (via Ask) so it serialises with Receive
// and the permanent/retryable verdict returns synchronously to the transit. This
// is the device→cell half of the relay path (the Forward seam is the cell→device
// half). Returns ErrNoCallbackTarget if no adapter owns the actor.
func (h *Host) DeliverCallbackFrame(frame behavior.ExternalCallbackFrame) error {
	cb, ok := h.callbacks[frame.AdapterActorID]
	if !ok {
		return ErrNoCallbackTarget
	}
	return h.cells.Ask(frame.AdapterActorID, func(ctx context.Context) error {
		return cb.HandleExternalCallbackFrame(ctx, frame)
	})
}

// ErrNoCallbackTarget is returned when a callback names an actor this host does
// not own as an adapter.
var ErrNoCallbackTarget = errNoCallbackTarget{}

type errNoCallbackTarget struct{}

func (errNoCallbackTarget) Error() string { return "host: no callback target for actor" }

// SpawnAgent hosts a worker-session agent as a kind=agent actor cell on this
// compute. Its chain is the uplink (no local truth), so the worker report the
// trigger emits flows UP to the home harness as the response. trigger is the
// worker-session backend seam (runtime/workerhost-driven); the cell just routes
// each inbound request to it serially. This is the agent half of "compute hosts
// cells" — the same hosting mechanism as InstallAdapter, for agents.
func (h *Host) SpawnAgent(self actor.ActorID, channelID channel.ID, trigger agentactor.TriggerFunc) {
	a := agentactor.New(agentactor.Deps{
		Self:      self,
		ChannelID: channelID,
		Chain:     NewUplinkChain(self, h.emit),
		Trigger:   trigger,
	})
	h.cells.Spawn(self, a)
}

// Dispatch routes an inbound envelope to the hosted cell's mailbox.
func (h *Host) Dispatch(ctx context.Context, frame computebus.DispatchFrame) error {
	return h.cells.Deliver(ctx, []actor.ActorID{frame.Target}, frame.Envelope)
}

// Cells exposes the runtime (for spawning agent cells / lifecycle).
func (h *Host) Cells() *actorrt.Runtime { return h.cells }

// Stop tears down all hosted cells.
func (h *Host) Stop() { h.cells.StopAll() }
