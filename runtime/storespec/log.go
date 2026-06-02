package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Seq is the messages.seq column — store-allocated monotonic per-channel
// sequence (L2 §1.4.1 PRIMARY KEY AUTOINCREMENT).
type Seq int64

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
// Concrete sqlite impl lives in runtime/store/messages.go. v2: no fencing
// parameter — the channel has a single writer (server harness) by
// construction (proto-v2-physical §4 / runtime-construction-spec §4.1).
type MessageLog interface {
	Append(ctx context.Context, env *message.Envelope) (AppendResult, error)
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
