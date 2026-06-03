package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Seq is the messages.seq column — store-allocated monotonic per-channel
// sequence (L2 §1.4.1 PRIMARY KEY AUTOINCREMENT).
type Seq int64

// StoredRow wraps a protocol Envelope with the store-derived columns kernel
// deliberately keeps OUT of the pure Envelope (they are store-derived, not
// protocol fields — kernel-construction-spec §1.2). Read paths return
// StoredRow; write paths (Append) take the pure envelope + the
// harness-computed is_terminal and the store allocates seq.
type StoredRow struct {
	Envelope message.Envelope

	Seq        int64
	IsTerminal bool
}

// AppendResult is what MessageLog.Append returns on a successful row write.
// It carries only what the STORE authoritatively produces and the caller
// could not already know: the store-allocated seq. (is_terminal is NOT echoed
// back — the harness COMPUTES it and passes it INTO Append, so reflecting it
// in the result would be a dead output mirroring the caller's own input.)
type AppendResult struct {
	// Seq is the store-allocated monotonic position (messages.seq).
	Seq Seq
}

// AppendError is the typed error returned for protocol-level rejects inside
// the engine append step (e.g. UNIQUE violation on terminal_response_per_
// request → terminal_duplicate).
//
// Reason is the WIRE STRING of a harness reject reason, NOT the typed
// harness.HarnessRejectReason — storespec must not import runtime/harness
// (that re-introduces the store↔harness cycle). The harness chain maps the
// string back to HarnessRejectReason at the boundary.
type AppendError struct {
	Reason           string
	Detail           string
	PartialMessageID message.ID
}

// Error implements the error interface.
func (e *AppendError) Error() string {
	if e.Detail != "" {
		return e.Reason + ": " + e.Detail
	}
	return e.Reason
}

// MessageLog is the channel-local messages-table append contract (L2
// §1.4.1). Append is the only mutation entry point; reads are not declared
// here because readers query messages through the MessageQuery role below.
//
// Concrete sqlite impl lives in runtime/internal/store/messages.go. v2 changes:
//   - no fencing parameter — the channel has a single writer (server
//     harness) by construction (proto-v2-physical §4).
//   - is_terminal is passed EXPLICITLY: kernel purified it off the Envelope
//     (it is store-derived, not a protocol field), so the harness — which
//     computes it in step 8 — hands it to Append. The store persists verbatim
//     (it stays the dumb persister, FIX-T10).
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope, isTerminal bool) (AppendResult, error)

	// FindByID returns the stored row for id (seq / is_terminal / envelope).
	// No channelID parameter: the store is bound to one channel at OpenChannel,
	// so a per-call channel arg is a pseudo-parameter — it implied a scoping the
	// query never performed (illegal-state-representable). The binding is the scope.
	FindByID(ctx context.Context, id message.ID) (*StoredRow, bool, error)

	// HasFinalResponse reports whether a final response already exists for
	// parentID (harness step 8 terminal uniqueness).
	HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error)
}

// MessageQuery is the channel-log READ role — segregated from MessageLog so a
// reader (client tail / closure supervisor) is handed a surface
// WITHOUT Append. Bundling reads with Append (one fat interface) would hand the
// harness-bypass write capability to every reader — the exact leak §4.5 closes;
// hence ISP/CQRS role-split, not one interface. The concrete satisfies both.
// The channel scope is the store assembly itself (one sqlite per channel), so
// no method re-takes a channel id — it would only re-specify what the file
// already fixes (and within one channel's file every row shares the channel_id).
type MessageQuery interface {
	// MaxSeq is the channel's highest seq (client cursor anchor).
	MaxSeq(ctx context.Context) (int64, error)
	// ReadAfterSeq is the client-push tail: envelopes with seq > afterSeq.
	ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]StoredRow, error)
	// OpenRequestsForActor returns ALL in-flight requests addressed to actorID.
	// It is the closure drain: the death-signal supervisor closes every one of a
	// dead actor's pending requests, so this is unbounded by construction — a
	// limit would silently leave the overflow callers hanging (no closure).
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]StoredRow, error)
}

// RequestLookup recovers an original request envelope by id (L2 §8 F5).
// Channel-scoped: implementations refuse cross-channel reads.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}
