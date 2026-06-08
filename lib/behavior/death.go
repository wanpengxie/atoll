package behavior

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// death.go holds substrate-death — closure author#3 — alongside the other two
// authors of the one behaviour base (author#1 in respond.go, author#2 in
// call.go; P13: three authors, one base). channelkit subscribes to the death
// edge and delegates here, injecting the seams.

// MaterialiseReceiverUnavailable is the DEATH author (author#3). For every
// in-flight request addressed to a dead actor it writes one
// receiver_unavailable terminal. sender = the channel system actor (the harness
// Step 8 authorises system + receiver_unavailable as the substrate author).
//
// onFault(reqID, err) lets the caller (channelkit) record each per-request
// closure fault — the base holds no logger. nil onFault = faults ignored.
//
// A drain-query failure returns error (NO caller can be closed — the loudest
// fault). A per-request build/write failure calls onFault and continues, so one
// bad request does not strand the rest.
func MaterialiseReceiverUnavailable(
	ctx context.Context,
	writer harness.Writer,
	query storespec.MessageQuery,
	clock func() time.Time,
	sender message.Sender,
	dead actor.ActorID,
	onFault func(reqID message.ID, err error),
) error {
	rows, err := query.OpenRequestsForActor(ctx, dead)
	if err != nil {
		return err
	}
	for i := range rows {
		req := &rows[i].Envelope
		if _, werr := Respond(ctx, writer, clock, req, sender, ResponseSpec{
			Status: "failed",
			Reason: string(message.TerminalReceiverUnavailable),
		}); werr != nil {
			if onFault != nil {
				onFault(req.ID, werr)
			}
		}
	}
	return nil
}
