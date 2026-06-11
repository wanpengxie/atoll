package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// stepID is the ordinal index inside the harness chain (proto-layer1 §2.0).
// Lower ids run first. The chain runner executes steps strictly ascending.
//
// (The former pure-contract harness package was deleted; these are the write
// ENGINE's own contracts, living with their consumer in runtime/harness —
// runtime-construction-spec §1.4. Step numbers are impl-shaped execution order,
// not protocol ADT.)
type stepID int

// Step 2 (StepDedupe) was retired with the v1 message-dedupe machinery;
// the ordinals are intentionally left non-contiguous (2 is a gap) so the
// remaining step numbers keep matching proto-layer1 §2.0 prose.
const (
	StepCallerAuth       stepID = 0
	StepEnvelopeShape    stepID = 1
	StepNormalize        stepID = 3
	StepSenderConsistent stepID = 4
	StepTypeRegistered   stepID = 5
	StepKindAndAudience  stepID = 7
	StepResponsePairing  stepID = 8
	StepEngineAppend     stepID = 9
)

// outcome describes the result of running one step against an envelope.
// Continue / Reject are the only two terminal outcomes for a single step.
type outcome struct {
	RejectReason HarnessRejectReason
	Detail       string

	// IsTerminal carries the harness-computed store-derived value from the
	// step that owns it (StepResponsePairing) up to the chain, which hands
	// it to MessageLog.Append. It replaces the deleted env.IsTerminal field
	// (kernel purified the envelope of store-derived columns).
	IsTerminal bool
}

// Continue is true when the step accepts the envelope and the next step runs.
func (o outcome) Continue() bool { return o.RejectReason == "" }

// step is one stage in the harness chain. Implementations live in this
// package (the step_*.go files).
type step interface {
	ID() stepID
	Run(ctx context.Context, env *message.Envelope) (outcome, error)
}

// WriteResult is the outcome of a full Chain.Write invocation (L1 §2).
// MessageID carries the envelope id on every path — durable on success,
// the attempted id on reject (Accepted()/RejectReason says which). There is
// no separate "partial" id field: the id is one value, the outcome flag tells
// you whether it became a durable row.
type WriteResult struct {
	MessageID    message.ID
	Seq          int64
	RejectReason HarnessRejectReason
	RejectDetail string
}

// Accepted reports whether the write produced a durable row.
func (r WriteResult) Accepted() bool { return r.RejectReason == "" }

// Writer is the harness write entry point as an interface — the injectable
// write seam for any binding edge that drives the chain. *Chain satisfies it.
type Writer interface {
	Write(ctx context.Context, env *message.Envelope) (WriteResult, error)
}
