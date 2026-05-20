package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepID is the ordinal index inside the 10-step harness chain
// (proto-layer1 §2.0). Lower numeric ids run first.
//
// The constant *names* mirror the spec vocabulary but the numeric
// values reflect the *physical* execution order chain.go enforces — the
// two diverge by one notable detail: StepSenderConsistent runs before
// StepDedupe and StepNormalize so the canonical hash used for dedupe is
// computed against the post-sender-consistent envelope (matches the
// store row that proto-layer1 §2.3 / §2.10 persist).
type StepID int

// Step ids per proto-layer1 §2.0 (Round 3 Cluster F). The new
// StepEnvelopeShape stage subsumes the pre-Round 3 RequiredFields step
// and adds visibility / audience cardinality / unknown-field guards.
const (
	StepCallerAuth       StepID = 0 // proto-layer1 §2.0 step 0+1 — caller principal + fence + channel binding
	StepEnvelopeShape    StepID = 1 // proto-layer1 §2.2 step 2  — required fields / closed sets / cardinality / unknown field fail-closed
	StepNormalize        StepID = 2 // proto-layer1 §2.4 step 4  — default-fill (audience/visibility/kind/correlation_id/payload/ts) + time-relation guard
	StepSenderConsistent StepID = 3 // proto-layer1 §2.6 step 6  — sender × caller match; sender.kind from registry
	StepTypeRegistered   StepID = 4 // proto-layer1 §2.5 step 5a — type ∈ (core ∪ registry)
	StepKindAndAudience  StepID = 5 // proto-layer1 §2.5/§2.7    — kind ∈ allowed_kinds + audience members active + handler match
	StepPayloadSchema    StepID = 6 // proto-layer1 §2.8 step 8  — payload schema validation
	StepDocRefs          StepID = 7 //                            — doc_refs path validation (envelope-level)
	StepResponsePairing  StepID = 8 // proto-layer1 §2.9 step 9  — response parent valid + The One Law uniqueness
	StepEngineAppend     StepID = 9 // proto-layer1 §2.10 step 10 — engine append + dispatch (terminal step; emits row)

	// StepDedupe (proto-layer1 §2.3) is the universal id-conflict pre-check.
	// chain.go inserts it between StepSenderConsistent and StepNormalize so
	// the canonical hash sees post-sender-consistent state (the same shape
	// the store row holds). It is NOT part of AllStepIDs because the spec
	// numbers it differently from the validation stages.
	StepDedupe StepID = -1
)

// AllStepIDs lists every numbered step in chain order. StepDedupe is
// excluded — callers that care about it reference it by name.
var AllStepIDs = []StepID{
	StepCallerAuth,
	StepEnvelopeShape,
	StepNormalize,
	StepSenderConsistent,
	StepTypeRegistered,
	StepKindAndAudience,
	StepPayloadSchema,
	StepDocRefs,
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
}

// Continue is true when the step accepts the envelope and the next
// step should run.
func (o Outcome) Continue() bool { return o.RejectReason == "" }

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
