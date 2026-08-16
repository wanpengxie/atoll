package actorbase

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// Pending is the single-use ticket sys.Call hands back — the caller's own
// out-station account entry (spec §1.5, the two-ledger account: this is the
// caller-facing read/write handle on ONE call ledger row, not a third table).
// It is a SEALED ticket, not a chaining builder: exactly two dispositions,
// nothing else grafts onto it later.
type Pending interface {
	// RequestID is the durable local request id allocated by Call. It is
	// available before the receiver reaches a terminal state so callers can
	// write correlation records at dispatch time.
	RequestID() message.ID
	// Wait blocks until the matching response lands, ctx is done, or d
	// elapses (selective receive on the out-station account) — whichever
	// first. ctx is caller-supplied by design (spec's ctx-provenance rule:
	// "只修 Wait 修不了生态" — a bare Proc's other calls (http/sql/SDK) all
	// take an explicit ctx too, so Wait does not get to be the one magic
	// exception). d bounds this particular wait window; it is independent of
	// (and typically much shorter than) the request's own durable deadline,
	// which the ledger enforces regardless of whether anyone is still
	// waiting. d<=0 means NO time bound — wait on ctx alone (pass msg.Ctx()
	// or a WithTimeout-derived ctx to bound it). NOTE the deliberate
	// asymmetry with JobTable.Await, where window<=0 means "do not wait at
	// all": a Proc holding a Pending is mid-flow and wants to park; a
	// mind-binding tool call is one dispatch and wants an immediate ack.
	// A wait parked when the account closes under it (deadline fired /
	// cancelled elsewhere) returns ErrCallClosed immediately.
	Wait(ctx context.Context, d time.Duration) (Msg, error)
	// Cancel closes this call's out-station entry early: it commits the
	// caller's OWN unanswered_timeout terminal now instead of waiting for the
	// deadline (a legal self-close, never a forged verdict — see spec
	// §1.5's pending.Cancel semantics), and lets a Canceller signal reach the
	// receiver's in-station account. Idempotent to a closed entry.
	Cancel() error
}
