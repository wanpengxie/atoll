package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// WriteResult is the outcome of a full Chain.Write invocation per L1
// §10.2 (the harness write entry point) and L2 §3.6 (binding-specific
// transport mapping — HTTP / Result.Err).
type WriteResult struct {
	// MessageID is the canonical envelope.id that was written (or
	// dedupe-matched). Empty on hard failures that occurred before id
	// finalization.
	MessageID message.ID

	// Seq is the store-derived monotonic sequence (L2 §1.4.1) — only
	// populated on successful append. Zero on dedupe / reject.
	Seq int64

	// Deduped reports the L2 §1.4.10.1 dedupe path: caller already wrote
	// this envelope.id earlier (idempotent retry); no new row was
	// inserted, returned id is the original.
	Deduped bool

	// RejectReason is set when one of the 9 steps rejected the write.
	// Empty means accept (or dedupe). See L1 §10.3.1 closed set.
	RejectReason RejectReason

	// RejectDetail mirrors Outcome.Detail from the rejecting step.
	RejectDetail string

	// PartialMessageID is set when reject happened after id finalization
	// (e.g. step 8 terminal_duplicate). Lets the caller observe which
	// id was actually allocated even though the row write failed.
	PartialMessageID message.ID
}

// Accepted reports whether the write produced a durable row (or matched
// an existing dedupe row).
func (r WriteResult) Accepted() bool { return r.RejectReason == "" }

// Chain is the Message-Write Harness top-level contract. Implementations
// (runtime/harness, T3) compose the 9 Step instances into the L1 §10.2
// validation chain.
//
// Kernel only declares the interface — it does not assemble or run the
// steps; that is the runtime's job. The Chain interface stays minimal
// because every harness implementation (daemon-rpc, in-worker bus,
// future tests) must agree on the entry-point shape.
type Chain interface {
	// Write executes Step 0..9 against `env` and returns the WriteResult.
	// The envelope is patched in-place during Step Normalize (so the
	// caller observes the post-normalize values via the same pointer).
	//
	// Implementations MUST:
	//   - Run the steps in StepID ascending order.
	//   - Short-circuit on the first reject (no later step runs).
	//   - Treat StepDedupe as an early short-circuit between Normalize
	//     and CallerAuth — already-written envelope.id returns Deduped.
	//   - Surface non-protocol errors (store IO, etc.) via the error
	//     return value, NOT inside WriteResult.RejectReason.
	Write(ctx context.Context, env *message.Envelope) (WriteResult, error)
}
