package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepID is the ordinal index inside the 9-step harness chain (L1
// §10.2). Step 0 is normalize (default-fill); steps 1..9 are validation
// stages.
type StepID int

// Step ids per L1 §10.2 (and L2 §3.4 — Authoritative Pseudocode
// reference). Keep this list 1:1 with the spec so renaming stays cheap.
const (
	StepNormalize        StepID = 0 // 0   — Normalize (audience/visibility/kind/correlation_id default-fill)
	StepCallerAuth       StepID = 1 // 1   — caller token / channel membership
	StepRequiredFields   StepID = 2 // 2   — L0 I1+I7 required fields + response.parent_id non-NULL
	StepSenderConsistent StepID = 3 // 3   — sender × caller match; sender.kind from registry; deregistered guard
	StepTypeRegistered   StepID = 4 // 4   — type ∈ (core ∪ registry)
	StepKindAndAudience  StepID = 5 // 5   — kind × type allowed_kinds + request audience narrow + handler match
	StepPayloadSchema    StepID = 6 // 6   — payload schema validation (per kind)
	StepDocRefs          StepID = 7 // 7   — doc_refs path validation
	StepResponsePairing  StepID = 8 // 8   — response parent valid + The One Law uniqueness
	StepEngineAppend     StepID = 9 // 9   — engine append + dispatch (terminal step; emits row)

	// StepDedupe (a.k.a. step 0.5 / step 0a) sits between Normalize and
	// CallerAuth — short-circuit when a row with envelope.id already
	// exists (hash-equal payload returns 200 dedupe). Kept named to mirror
	// L1 §10.2 / L2 §1.4.10.1 vocabulary even though it is not a numbered
	// validation stage.
	StepDedupe StepID = -1
)

// AllStepIDs lists every numbered step (0..9) in chain order. StepDedupe
// is excluded — callers that care about it reference it by name.
var AllStepIDs = []StepID{
	StepNormalize,
	StepCallerAuth,
	StepRequiredFields,
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
	// RejectReason is set when the step rejects the write per L1 §10.3.
	// Empty string == continue.
	RejectReason RejectReason

	// Detail is an informative error string. Caller MAY surface it in
	// logs / Result.Err / RPC error body — protocol does not require it.
	Detail string

	// PartialMessageID is set on rejects that occurred AFTER the
	// envelope.id was finalized (e.g. step 8 terminal_duplicate happens
	// in the engine append transaction). Empty otherwise.
	PartialMessageID message.ID
}

// Continue is true when the step accepts the envelope and the next
// step should run.
func (o Outcome) Continue() bool { return o.RejectReason == "" }

// Step is one stage in the 9-step harness chain. Implementations live
// in runtime/harness — kernel only declares the interface so concrete
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
