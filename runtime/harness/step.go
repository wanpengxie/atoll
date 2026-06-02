package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// StepID is the ordinal index inside the harness chain (proto-layer1 §2.0).
// Lower ids run first. The chain runner executes steps strictly ascending.
//
// (Relocated from the deleted kernel/harness — these are the write ENGINE's
// own contracts, runtime-construction-spec §1.4. Step numbers are impl-shaped
// execution order, not protocol ADT.)
type StepID int

const (
	StepCallerAuth       StepID = 0
	StepEnvelopeShape    StepID = 1
	StepDedupe           StepID = 2
	StepNormalize        StepID = 3
	StepSenderConsistent StepID = 4
	StepTypeRegistered   StepID = 5
	StepAudienceResolve  StepID = 6
	StepKindAndAudience  StepID = 7
	StepResponsePairing  StepID = 8
	StepEngineAppend     StepID = 9
)

// AllStepIDs lists every step in physical execution order.
var AllStepIDs = []StepID{
	StepCallerAuth,
	StepEnvelopeShape,
	StepDedupe,
	StepNormalize,
	StepSenderConsistent,
	StepTypeRegistered,
	StepAudienceResolve,
	StepKindAndAudience,
	StepResponsePairing,
	StepEngineAppend,
}

// Outcome describes the result of running one step against an envelope.
// Continue / Reject are the only two terminal outcomes for a single step.
type Outcome struct {
	RejectReason       HarnessRejectReason
	Detail             string
	PartialMessageID   message.ID
	Deduped            bool
	ExistingSeq        int64
	ExistingIsTerminal bool
	ExistingTSReceived int64

	// IsTerminal / CanonicalHash carry the harness-computed store-derived
	// values from the step that owns them (StepResponsePairing computes
	// IsTerminal; StepDedupe computes CanonicalHash) up to the chain, which
	// hands them to MessageLog.Append. They replace the deleted
	// env.IsTerminal / env.CanonicalHash fields (kernel purified the
	// envelope of store-derived columns).
	IsTerminal    bool
	CanonicalHash string
}

// Continue is true when the step accepts the envelope and the next step runs.
func (o Outcome) Continue() bool { return o.RejectReason == "" && !o.Deduped }

// Step is one stage in the harness chain. Implementations live in this
// package (the step_*.go files).
type Step interface {
	ID() StepID
	Run(ctx context.Context, env *message.Envelope) (Outcome, error)
}

// WriteResult is the outcome of a full Chain.Write invocation (L1 §2).
type WriteResult struct {
	MessageID        message.ID
	Seq              int64
	Deduped          bool
	RejectReason     HarnessRejectReason
	RejectDetail     string
	PartialMessageID message.ID
}

// Accepted reports whether the write produced a durable row (or matched an
// existing dedupe row).
func (r WriteResult) Accepted() bool { return r.RejectReason == "" }

// Writer is the harness write entry point as an interface, for callers that
// inject the chain (typeinstall, lib install behaviour, control handlers).
// *Chain satisfies it. (Replaces the deleted kernel/harness.Chain interface.)
type Writer interface {
	Write(ctx context.Context, env *message.Envelope) (WriteResult, error)
}
