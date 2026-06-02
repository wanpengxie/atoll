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
// here because agents / scheduler / trigger may query messages directly.
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
	FindByID(ctx context.Context, channelID channel.ID, id message.ID) (*StoredRow, bool, error)

	// HasFinalResponse reports whether a final response already exists for
	// parentID (harness step 8 terminal uniqueness).
	HasFinalResponse(ctx context.Context, channelID channel.ID, parentID message.ID) (bool, error)

	// FinalResponseSender returns the sender of the existing final response
	// for parentID, if any (late-final detection).
	FinalResponseSender(ctx context.Context, channelID channel.ID, parentID message.ID) (actor.ActorID, bool, error)
}

// MessageQuery is the channel-log READ role — segregated from MessageLog so a
// reader (scheduler / client tail / closure supervisor) is handed a surface
// WITHOUT Append. Bundling reads with Append (one fat interface) would hand the
// harness-bypass write capability to every reader — the exact leak §4.5 closes;
// hence ISP/CQRS role-split, not one interface. The concrete satisfies both.
type MessageQuery interface {
	// MaxSeq is the channel's highest seq (client cursor anchor).
	MaxSeq(ctx context.Context, channelID channel.ID) (int64, error)
	// ReadAfterSeq is the client-push tail: envelopes with seq > afterSeq.
	ReadAfterSeq(ctx context.Context, channelID channel.ID, afterSeq int64, limit int) ([]StoredRow, error)
	// OpenRequestsForActor returns in-flight requests addressed to actorID.
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID, limit int) ([]StoredRow, error)
}

// Cursor mirrors an actor_cursors row (L2 §1.4.3). The position metric is
// LastConsumedSeq — the one monotonic truth of how far an actor has consumed.
// (No last-consumed message id: it was never decision-read; seq is the
// position and the id is a derivable label, not cursor truth.)
type Cursor struct {
	ActorID         actor.ActorID
	LastConsumedSeq Seq
	UpdatedAt       int64
}

// Cursors is the actor_cursors query / mutation contract. Concrete impl in
// runtime/store/actors.go; Advance MUST honor the L1 §6.3.4.3 monotonic CAS.
type Cursors interface {
	Get(ctx context.Context, actorID actor.ActorID) (Cursor, bool, error)
	Advance(ctx context.Context, actorID actor.ActorID, newSeq Seq, nowMs int64) (ok bool, err error)
}

// RequestLookup recovers an original request envelope by id (L2 §8 F5).
// Channel-scoped: implementations refuse cross-channel reads.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}
