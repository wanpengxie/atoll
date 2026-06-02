// Package host runs business actor cells on an attached compute (v2). See doc.go.
package host

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// Host hosts business cells (tool/agent actors) on a compute. It maps a
// DispatchFrame from the home to the cell mailbox, and each cell's output flows
// UP via an UplinkChain (NO local truth — daemon is attached compute).
type Host struct {
	cells *actorrt.Runtime
	emit  EmitFunc
}

// New constructs a host. emit is the homelink-injected uplink to the home harness.
func New(emit EmitFunc) *Host {
	return &Host{
		cells: actorrt.New(actorrt.Config{}),
		emit:  emit,
	}
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
	return res.ActorID, nil
}

// Dispatch routes an inbound envelope to the hosted cell's mailbox.
func (h *Host) Dispatch(ctx context.Context, frame computebus.DispatchFrame) error {
	return h.cells.Deliver(ctx, []actor.ActorID{frame.Target}, frame.Envelope)
}

// Cells exposes the runtime (for spawning agent cells / lifecycle).
func (h *Host) Cells() *actorrt.Runtime { return h.cells }

// Stop tears down all hosted cells.
func (h *Host) Stop() { h.cells.StopAll() }
