package host

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// EmitFunc sends a business cell's output UP to the channel home and returns the
// home harness's EmitAck (the authoritative WriteResult). homelink injects a
// computebus-backed implementation that blocks for the ack.
type EmitFunc func(ctx context.Context, frame computebus.EmitFrame) (computebus.EmitAck, error)

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

// Write forwards env to the home harness and returns the authoritative
// WriteResult carried back in the EmitAck (the home ran the 9 steps + wrote
// truth). The compute cell's Respond/EmitEvent thus observes the real outcome.
func (u UplinkChain) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	ack, err := u.emit(ctx, computebus.EmitFrame{Source: u.self, Envelope: env})
	if err != nil {
		return harness.WriteResult{}, err
	}
	res := harness.WriteResult{
		MessageID:    ack.MessageID,
		RejectReason: harness.RejectReason(ack.RejectReason),
	}
	if ack.Err != "" {
		return res, errors.New(ack.Err)
	}
	return res, nil
}
