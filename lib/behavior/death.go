package behavior

import (
	"context"
	"fmt"
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
// receiver_unavailable terminal. The pen is the system pen (the harness Step 8
// authorises system + receiver_unavailable as the substrate author); the system
// identity is welded onto the pen (sealed-pen), never a parameter.
//
// onFault(reqID, err) lets the caller (channelkit) record each per-request
// closure fault — the base holds no logger. nil onFault = faults ignored.
//
// A drain-query failure returns error (NO caller can be closed — the loudest
// fault). A per-request build/write failure calls onFault and continues, so one
// bad request does not strand the rest.
func MaterialiseReceiverUnavailable(
	ctx context.Context,
	pen harness.Pen,
	query storespec.MessageQuery,
	clock func() time.Time,
	dead actor.ActorID,
	onFault func(reqID message.ID, err error),
) error {
	rows, err := query.OpenRequestsForActor(ctx, dead)
	if err != nil {
		return err
	}
	for i := range rows {
		req := &rows[i].Envelope
		if _, werr := Respond(ctx, pen, clock, req, ResponseSpec{
			Status: message.StatusFailed,
			Reason: string(message.TerminalReceiverUnavailable),
		}); werr != nil {
			if onFault != nil {
				onFault(req.ID, werr)
			}
		}
	}
	return nil
}

// LivenessProbe answers the substrate's authoritative "is a live instance for id
// hosted right now" question — the `kill -0` / is_process_alive authority only
// the substrate can answer. The reconciler uses it to decide which open-request
// receivers are absent. present=false means closure is owed.
type LivenessProbe interface {
	Present(id actor.ActorID) bool
}

// ReconcileReceiverUnavailable is the closure RECONCILER (the level-triggered
// companion to the death edge). Closure is not edge-only: a death edge can be
// lost (clean despawn, ctx-cancel, a home restart that predates the open
// request), and an open request whose receiver is absent must STILL be closed.
// So closure is a level scan over truth: enumerate every receiver that holds an
// open request, and for each one the substrate reports ABSENT, drain it via
// MaterialiseReceiverUnavailable. It is the same author#3 terminal, reached by a
// scan instead of an edge.
//
// Idempotent by construction: receiver_unavailable is a final terminal, so a
// re-scan re-writing one collides with the ux_terminal_response_per_request
// UNIQUE index and is rejected; a receiver that has since answered (completed
// landed first) likewise collides. So this scan is safe to run any time, any
// number of times — startup, a low-frequency ticker, or a lossy-edge wakeup.
//
// receivers() is the truth-derived set of distinct open-request receivers (a
// derived view over the log, not membership). present probes substrate liveness.
// onFault receives both a drain-query failure (the loudest — that receiver's
// callers are all black holes) and any per-request write fault.
func ReconcileReceiverUnavailable(
	ctx context.Context,
	pen harness.Pen,
	query storespec.MessageQuery,
	present LivenessProbe,
	clock func() time.Time,
	onFault func(reqID message.ID, err error),
) error {
	receivers, err := query.DistinctOpenRequestReceivers(ctx)
	if err != nil {
		return err
	}
	for _, id := range receivers {
		if present.Present(id) {
			continue // receiver is live — no closure owed, it can still answer.
		}
		if derr := MaterialiseReceiverUnavailable(ctx, pen, query, clock, id, onFault); derr != nil {
			// Per-receiver drain-query failure: surface it (every one of this
			// receiver's callers is a black hole) and continue — one bad receiver
			// must not strand the rest of the scan. This fault is per-RECEIVER, not
			// per-request (the drain failed before any request was enumerated), so
			// the reqID slot stays empty and the receiver id rides in the error —
			// never pun an ActorID into the request-id position.
			if onFault != nil {
				onFault("", fmt.Errorf("reconcile drain receiver %s: %w", id, derr))
			}
		}
	}
	return nil
}
