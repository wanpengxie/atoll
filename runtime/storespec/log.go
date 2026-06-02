package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Seq is the messages.seq column — store-allocated monotonic per-channel
// sequence (L2 §1.4.1 PRIMARY KEY AUTOINCREMENT).
type Seq int64

// StoredRow wraps a protocol Envelope (17 content+metadata fields) with the
// store-derived columns kernel deliberately keeps OUT of the pure Envelope
// (they are store-derived, not protocol fields — kernel-construction-spec
// §1.2). Read paths return StoredRow; write paths (Append) take the pure
// envelope + the harness-computed is_terminal / canonical_hash and the store
// allocates seq. (DeliveredAt / LastError stay on the Envelope — L0 §2.5
// delivery metadata, part of the wire envelope.)
type StoredRow struct {
	Envelope message.Envelope

	Seq              int64
	IsTerminal       bool
	CanonicalHash    string
	Attempts         int64
	DeliveryFailedAt *int64
}

// AppendResult is what MessageLog.Append returns on a successful row write
// (or a dedupe hit on an existing envelope.id).
type AppendResult struct {
	// Seq is the store-allocated monotonic position (messages.seq).
	Seq Seq
	// IsTerminal mirrors the row's is_terminal column (computed from
	// payload.status at insert time, proto-layer0 §2.5.1).
	IsTerminal bool
	// Deduped reports whether the append matched an existing row by id
	// (L2 §1.4.10.1 / harness dedupe path).
	Deduped bool
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
// here because agents / scheduler / trigger may query messages directly.
//
// Concrete sqlite impl lives in runtime/internal/store/messages.go. v2 changes:
//   - no fencing parameter — the channel has a single writer (server
//     harness) by construction (proto-v2-physical §4).
//   - is_terminal + canonical_hash are passed EXPLICITLY: kernel purified
//     them off the Envelope (they are store-derived, not protocol fields),
//     so the harness — which computes them in step 8 / step dedupe — hands
//     them to Append. The store persists verbatim (it stays the dumb
//     persister, FIX-T10).
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope, isTerminal bool, canonicalHash string) (AppendResult, error)

	// LookupCanonicalHash returns the stored canonical_hash for id (used by
	// harness step dedupe to verify an idempotent retry).
	LookupCanonicalHash(ctx context.Context, channelID channel.ID, id message.ID) (string, bool, error)

	// FindByID returns the stored row for id (seq / is_terminal / envelope).
	FindByID(ctx context.Context, channelID channel.ID, id message.ID) (*StoredRow, bool, error)

	// HasFinalResponse reports whether a final response already exists for
	// parentID (harness step 8 terminal uniqueness).
	HasFinalResponse(ctx context.Context, channelID channel.ID, parentID message.ID) (bool, error)

	// FinalResponseSender returns the sender of the existing final response
	// for parentID, if any (late-final detection).
	FinalResponseSender(ctx context.Context, channelID channel.ID, parentID message.ID) (actor.ActorID, bool, error)
}

// Cursor mirrors an actor_cursors row (L2 §1.4.3). Position metric is
// LastConsumedSeq; LastConsumedID is informational only.
type Cursor struct {
	ActorID         actor.ActorID
	LastConsumedSeq Seq
	LastConsumedID  message.ID
	UpdatedAt       int64
}

// Cursors is the actor_cursors query / mutation contract. Concrete impl in
// runtime/store/actors.go; Advance MUST honor the L1 §6.3.4.3 monotonic CAS.
type Cursors interface {
	Get(ctx context.Context, actorID actor.ActorID) (Cursor, bool, error)
	Advance(ctx context.Context, actorID actor.ActorID, newSeq Seq, newID message.ID, nowMs int64) (ok bool, err error)
}

// RequestLookup recovers an original request envelope by id (L2 §8 F5).
// Channel-scoped: implementations refuse cross-channel reads.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}
