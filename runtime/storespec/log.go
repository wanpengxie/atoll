package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
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
// Reason is a plain-string diagnostic code for a protocol-level violation the
// store detected at write time (e.g. terminal_duplicate). The concrete values
// are defined by the store implementation per violation type; the consumer is
// responsible for interpreting them / mapping into its own error domain.
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
//   - no fencing parameter — the store is scoped to one channel at construction
//     time, and that scope is the unique write path, so an extra fencing token
//     would only re-assert what construction already fixes.
//   - is_terminal is passed EXPLICITLY: kernel purified it off the Envelope
//     (it is store-derived, not a protocol field). It is computed by the caller
//     because it depends on message-kind semantics the store does not interpret;
//     the store persists it verbatim (it stays the dumb persister, FIX-T10).
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope, isTerminal bool) (AppendResult, error)

	// FindByID returns the stored row for id (seq / is_terminal / envelope).
	// No channelID parameter: the store is bound to one channel at OpenChannel,
	// so a per-call channel arg is a pseudo-parameter — it implied a scoping the
	// query never performed (illegal-state-representable). The binding is the scope.
	FindByID(ctx context.Context, id message.ID) (*StoredRow, bool, error)

	// HasFinalResponse reports whether a terminal (final) response row already
	// exists for parentID — the store-level pre-check for the
	// one-terminal-response-per-request invariant.
	HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error)
}

// MessageQuery is the channel-log READ role — segregated from MessageLog so a
// read-only consumer receives only the read surface, WITHOUT Append. Bundling
// reads with Append (one fat interface) would grant write capability to every
// reader; the ISP/CQRS role-split keeps a reader unable to reach the write
// path. The concrete satisfies both. The channel scope is the store assembly
// itself (one sqlite per channel), so no method re-takes a channel id — it
// would only re-specify what the file already fixes (and within one channel's
// file every row shares the channel_id).
type MessageQuery interface {
	// MaxSeq is the channel's highest committed seq.
	MaxSeq(ctx context.Context) (int64, error)
	// ReadAfterSeq returns envelopes with seq > afterSeq, in ascending seq order.
	ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]StoredRow, error)
	// OpenRequestsForActor returns ALL open requests addressed to actorID.
	// It is the closure drain: closing a dead actor's in-flight requests must
	// drain every one of them, so this is unbounded by construction — a limit
	// would silently leave the overflow callers hanging (no closure).
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]StoredRow, error)

	// DistinctOpenRequestReceivers returns the set of receivers (first-audience
	// member) that still have at least one open request. It is the truth-derived
	// view the closure RECONCILER scans: closure is a level-triggered reconciler
	// (orphan open-request × receiver-absent → receiver_unavailable), and the
	// authoritative "who has an open request" question is answered by the message
	// log, not by membership (a member with no open request needs no closure; an
	// open request is the only thing that demands one). The reconciler intersects
	// this set with substrate presence to find absent receivers, then drains each
	// via OpenRequestsForActor. Unbounded by construction (same closure law).
	DistinctOpenRequestReceivers(ctx context.Context) ([]actor.ActorID, error)
}

// RequestLookup recovers an original request envelope by id (L2 §8 F5).
// Channel-scoped: implementations refuse cross-channel reads.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}
