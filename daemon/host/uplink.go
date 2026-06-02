package host

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// EmitFunc sends a business cell's output UP to the channel home (homelink
// injects a computebus-backed implementation).
type EmitFunc func(ctx context.Context, frame computebus.EmitFrame) error

// UplinkChain implements kernel/harness.Chain by forwarding each write UP to the
// channel home harness as a computebus.EmitFrame. A compute cell has NO local
// truth — its adapterActor.chain is this uplink, so Respond/Provisional/EmitEvent
// flow to the server harness (which owns truth + runs the 9 steps) rather than
// writing locally. This is the v2 truth-flip on the compute side.
type UplinkChain struct {
	self actor.ActorID
	emit EmitFunc
}

// NewUplinkChain binds an uplink for one source actor.
func NewUplinkChain(self actor.ActorID, emit EmitFunc) UplinkChain {
	return UplinkChain{self: self, emit: emit}
}

// Write forwards env to the home harness. v1: optimistic — the server harness is
// authoritative; full emit-ack (returning the server WriteResult over the wire)
// lands with the computebus request/reply. P9.
func (u UplinkChain) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if err := u.emit(ctx, computebus.EmitFrame{Source: u.self, Envelope: env}); err != nil {
		return harness.WriteResult{}, err
	}
	return harness.WriteResult{MessageID: env.ID}, nil
}
