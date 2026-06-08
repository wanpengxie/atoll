package host

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// EmitFunc sends a cell's output UP to the channel home and returns the home
// harness's EmitAck (the authoritative WriteResult). homelink.Emit injects a
// computebus-backed implementation that blocks for the ack.
type EmitFunc func(ctx context.Context, frame computebus.EmitFrame) (computebus.EmitAck, error)

// UplinkWriter satisfies harness.Writer by forwarding each Write UP to the
// channel home as a computebus.EmitFrame. A daemon cell has NO local truth --
// its writer is this uplink, so behavior.Respond / behavior.EmitEvent flow to
// the server harness (which owns truth + runs the 9 steps) rather than writing
// locally. This is the v2 truth-flip on the compute side.
//
// Correlation (EmitID) and timeout are homelink's concern; UplinkWriter just
// blocks on the EmitFunc return.
type UplinkWriter struct {
	self actor.ActorID
	emit EmitFunc
}

// NewUplinkWriter binds an uplink writer for one source actor.
func NewUplinkWriter(self actor.ActorID, emit EmitFunc) *UplinkWriter {
	return &UplinkWriter{self: self, emit: emit}
}

// Write forwards env to the home harness and returns the authoritative
// WriteResult carried back in the EmitAck (the home ran the 9 steps and wrote
// truth). The compute cell's Respond/EmitEvent observes the real outcome.
func (u *UplinkWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	ack, err := u.emit(ctx, computebus.EmitFrame{Source: u.self, Envelope: env})
	if err != nil {
		return harness.WriteResult{}, err
	}
	res := harness.WriteResult{
		MessageID:    ack.MessageID,
		RejectReason: harness.HarnessRejectReason(ack.RejectReason),
	}
	if ack.Err != "" {
		return res, errors.New(ack.Err)
	}
	return res, nil
}

// Verify interface satisfaction at compile time.
var _ harness.Writer = (*UplinkWriter)(nil)
