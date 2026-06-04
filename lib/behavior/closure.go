package behavior

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// closure.go holds substrate-death — closure author#3 — co-located with the
// other two authors (P13: three authors, one base). The materialisation logic
// moved here from channelkit; channelkit now only subscribes to the death edge
// and delegates, injecting the seams.

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
	writer ResponseWriter,
	openReqs OpenRequests,
	clock func() time.Time,
	sender message.Sender,
	dead actor.ActorID,
	onFault func(reqID message.ID, err error),
) error {
	reqs, err := openReqs.OpenRequestsForActor(ctx, dead)
	if err != nil {
		return err
	}
	for _, req := range reqs {
		if req == nil {
			continue
		}
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

// isFinalResponse reports whether env is a final (terminal) response. It parses
// env.payload.status and defers to message.IsFinalStatus. Internal helper used
// by Caller.Match to decide closure. A non-response or unparseable payload is
// not final.
func isFinalResponse(env *message.Envelope) bool {
	if env == nil || env.Kind != message.KindResponse {
		return false
	}
	var p struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return false
	}
	return message.IsFinalStatus(p.Status)
}
