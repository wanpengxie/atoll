package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepID is the ordinal index inside the harness chain (proto-layer1
// §2.0). Lower numeric ids run first. The chain runner executes steps
// strictly in ascending ID order — there are no out-of-band steps.
//
// Step numbers loosely mirror the spec's §2.0 sequence (the spec lists
// Step 0…Step 10; code collapses Step 0 + Step 1 into StepCallerAuth and
// splits Step 5/7 into separate StepTypeRegistered + StepKindAndAudience
// stages for engineering granularity). What matters protocol-wise is
// physical execution ordering, which this file fixes.
type StepID int

// Step ids per proto-layer1 §2.0. Round 3 placed StepDedupe before
// StepNormalize so the canonical hash sees sender-provided fields
// (pre-normalize). See proto-layer1 §2.3 for the rationale.
const (
	StepCallerAuth       StepID = 0 // proto-layer1 §2.0 step 0+1 — caller principal + fence + channel binding
	StepEnvelopeShape    StepID = 1 // proto-layer1 §2.2 step 2  — required fields / closed sets / cardinality / unknown field fail-closed
	StepDedupe           StepID = 2 // proto-layer1 §2.3 step 3  — id dedupe over sender-provided fields (pre-normalize)
	StepNormalize        StepID = 3 // proto-layer1 §2.4 step 4  — default-fill (audience/visibility/kind/correlation_id/payload/ts) + time-relation guard
	StepSenderConsistent StepID = 4 // proto-layer1 §2.6 step 6  — sender × caller match; sender.kind from registry
	StepTypeRegistered   StepID = 5 // proto-layer1 §2.5 step 5a — type ∈ (core ∪ registry) + reserved namespace authority
	StepKindAndAudience  StepID = 6 // proto-layer1 §2.5/§2.7    — kind ∈ allowed_kinds + audience members active + handler match
	StepPayloadSchema    StepID = 7 // proto-layer1 §2.8 step 8  — payload schema validation
	StepResponsePairing  StepID = 8 // proto-layer1 §2.9 step 9  — response parent valid + The One Law uniqueness
	StepEngineAppend     StepID = 9 // proto-layer1 §2.10 step 10 — engine append + dispatch (terminal step; emits row)
)

// AllStepIDs lists every step in physical execution order. The chain
// runner sorts by ID before invoking, so this slice is the canonical
// reference.
var AllStepIDs = []StepID{
	StepCallerAuth,
	StepEnvelopeShape,
	StepDedupe,
	StepNormalize,
	StepSenderConsistent,
	StepTypeRegistered,
	StepKindAndAudience,
	StepPayloadSchema,
	StepResponsePairing,
	StepEngineAppend,
}

// Outcome describes the result of running one step against an envelope.
//
// Continue / Reject are the only two terminal outcomes for a single
// step. The chain runner short-circuits on Reject and runs the next
// step on Continue. Step-level errors that are NOT protocol rejects
// (e.g. store IO failure) are returned via the error return value of
// Step.Run, not by stuffing them into Outcome.
type Outcome struct {
	// RejectReason is set when the step rejects the write per
	// proto-layer1 §2.11. Empty string == continue.
	RejectReason RejectReason

	// Detail is an informative error string. Caller MAY surface it in
	// logs / Result.Err / RPC error body — protocol does not require it.
	Detail string

	// PartialMessageID is set on rejects that occurred AFTER the
	// envelope.id was finalized (e.g. step 9 terminal_duplicate happens
	// in the engine append transaction). Empty otherwise.
	PartialMessageID message.ID

	// Deduped is set by StepDedupe when the incoming envelope is an
	// idempotent retry of an existing row. Caller MUST short-circuit the
	// remaining steps and surface the existing row's seq.
	Deduped bool

	// ExistingSeq is the stored seq of the existing row when Deduped is
	// true. Zero otherwise.
	ExistingSeq int64

	// ExistingIsTerminal mirrors the stored is_terminal flag for a
	// dedupe-hit row. Zero/false otherwise.
	ExistingIsTerminal bool

	// ExistingTSReceived mirrors the stored ts_received timestamp for a
	// dedupe-hit row. Zero otherwise.
	ExistingTSReceived int64
}

// Continue is true when the step accepts the envelope and the next
// step should run.
func (o Outcome) Continue() bool { return o.RejectReason == "" && !o.Deduped }

// Step is one stage in the harness chain. Implementations live in
// runtime/harness — kernel only declares the interface so concrete
// step rules can be swapped out (e.g. M1.x adds new steps without
// touching the kernel layer).
type Step interface {
	// ID returns the chain ordinal — used by Chain to assemble the
	// stages in stable order and by tests to assert step coverage.
	ID() StepID

	// Run executes the step's logic against `env` (which the chain
	// passes by-pointer so Step.Run may patch it during normalize).
	// `ctx` carries trigger context, caller token, channel id, etc.
	// (the actual context-key contract lives in runtime/harness).
	//
	// Returns:
	//   - Outcome (Continue or Reject + reason)
	//   - error  for non-protocol failures (store IO, etc.) — chain
	//            aborts and surfaces error to caller untouched.
	Run(ctx context.Context, env *message.Envelope) (Outcome, error)
}
