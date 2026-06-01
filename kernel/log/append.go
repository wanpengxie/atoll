package log

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/fencing"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// AppendResult is what MessageLog.Append returns when the row write
// succeeds (or when dedupe matches an existing envelope.id). For hard
// failures the error return value is non-nil and AppendResult is the
// zero value.
type AppendResult struct {
	// Seq is the store-allocated monotonic position (messages.seq).
	Seq Seq

	// IsTerminal mirrors the row's `is_terminal` column (L2 §1.4.1) —
	// computed from payload.status at insert time (proto-layer0 §2.5.1).
	IsTerminal bool

	// Deduped reports whether the append matched an existing row by
	// envelope.id (L2 §1.4.10.1 / harness step 0.5 dedupe path).
	Deduped bool
}

// FencingTuple is the explicit daemon ownership token a channel-local
// MessageLog append must present when the concrete store enforces fencing.
// A zero tuple means "no fencing supplied"; unfenced test stores may ignore
// it, but fenced stores reject it as stale rather than reading hidden state
// from context.Context.
type FencingTuple struct {
	Token fencing.FencingToken
	Epoch fencing.DaemonEpoch
}

// AppendError is the typed error returned for protocol-level rejects
// inside the engine append step (e.g. UNIQUE constraint violation on
// terminal_response_per_request → terminal_duplicate). Wrapping in a
// typed error lets the harness chain map it to message.HarnessRejectReason
// without string parsing.
type AppendError struct {
	Reason message.HarnessRejectReason
	Detail string
	// PartialMessageID is the envelope.id that was attempted (so the
	// caller can report which id "lost" the race).
	PartialMessageID message.ID
}

// Error implements the error interface.
func (e *AppendError) Error() string {
	if e.Detail != "" {
		return string(e.Reason) + ": " + e.Detail
	}
	return string(e.Reason)
}

// MessageLog is the channel-local messages-table append contract (L2
// §1.4.1). Append is the only mutation entry point; reads are not
// declared here because L2 §2.2 explicitly allows agents / scheduler /
// trigger gateway to read messages directly via sqlite query.
//
// Concrete sqlite implementation lives in runtime/store/messages.go
// (T3) and MUST enforce the L2 §1.4.1 invariants:
//   - id UNIQUE
//   - parent_id terminal-response UNIQUE INDEX
//   - same-transaction is_terminal computation
//
// Engine append ACL (L2 §1.4.5) is layered ABOVE this interface —
// only the harness chain may call Append; other call paths are
// rejected by runtime/harness, not by kernel/log.
type MessageLog interface {
	// Append writes the envelope to the channel-local messages table.
	// Returns *AppendError for protocol-level rejects (terminal
	// duplicate / id conflict), or a generic error for IO failures.
	//
	// On success, AppendResult.Seq is the row's store-allocated seq;
	// the implementation MAY also patch env.Seq + env.IsTerminal +
	// env.TSReceived in-place so the harness chain can return the
	// final envelope to the caller (L0 §3.2 — engine writes
	// ts_received).
	Append(ctx context.Context, env *message.Envelope, fencing FencingTuple) (AppendResult, error)

	// FindByID returns the row identified by envelope.id, or ok=false
	// when no such row exists. Used by callers that need the full
	// envelope (parent lookup, view fanout, recovery).
	FindByID(ctx context.Context, channelID channel.ID, id message.ID) (message.Envelope, bool, error)

	// LookupCanonicalHash returns the stored canonical_hash of the row
	// identified by envelope.id, or ok=false when no such row exists.
	// Used by harness StepDedupe (proto-layer1 §2.3) to compare a retry
	// against the stored sender-provided hash without recomputing from
	// the post-normalize row.
	LookupCanonicalHash(ctx context.Context, channelID channel.ID, id message.ID) (hash string, ok bool, err error)

	// HasFinalResponse reports whether the channel log already contains
	// a kind=response row pointing at parentID with payload.status in
	// the Layer 1 final closed set ({"completed","failed"}). It is the
	// non-authoritative pre-check harness Step 8 uses to distinguish
	// final-after-final (→ harness_terminal_duplicate) from
	// provisional-after-final (→ harness_provisional_after_final). The
	// engine append UNIQUE INDEX on (parent_id) WHERE is_terminal=1 is
	// the authoritative defence against the former; this lookup is the
	// only way to surface the latter, which the partial index cannot
	// catch by definition.
	HasFinalResponse(ctx context.Context, channelID channel.ID, parentID message.ID) (bool, error)

	// FinalResponseSender returns the sender.id of the existing Layer 1
	// final response for parentID, or ok=false when no final exists. Used
	// by harness Step 8 to distinguish a caller self-close
	// (unanswered_timeout, sender == parent request sender) from a genuine
	// receiver final, so a receiver's LATE final after a caller self-close
	// can be rewritten to a response.late_final observability event
	// (actor-runtime-redesign.md §0.5 Δ4 / D3) rather than rejected as a
	// duplicate.
	FinalResponseSender(ctx context.Context, channelID channel.ID, parentID message.ID) (actor.ActorID, bool, error)
}
