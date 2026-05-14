package log

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/channel"
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
	// computed from type_registry.terminal_convention + payload at
	// insert time.
	IsTerminal bool

	// Deduped reports whether the append matched an existing row by
	// envelope.id (L2 §1.4.10.1 / harness step 0.5 dedupe path).
	Deduped bool
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
	PartialMessageID string
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
	Append(ctx context.Context, env *message.Envelope) (AppendResult, error)

	// FindByID returns the row identified by envelope.id, or ok=false
	// when no such row exists. Used by harness step 0.5 (dedupe path)
	// to compare canonical-hash before short-circuiting.
	FindByID(ctx context.Context, channelID channel.ID, id string) (message.Envelope, bool, error)
}
