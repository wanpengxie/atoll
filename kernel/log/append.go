package log

import (
	"github.com/wanpengxie/ActOS/kernel/message"
)

// AppendResult is what a MessageLog.Append returns when the row write
// succeeds (or when dedupe matches an existing envelope.id). For hard
// failures the error return value is non-nil and AppendResult is the
// zero value. (The MessageLog contract itself lives in runtime/harness —
// the write-chain consumer — so kernel/log carries only the pure result/
// error types it shares with the store implementation.)
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
