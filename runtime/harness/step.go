package harness

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// stepID is the ordinal index inside the harness chain. Lower ids run
// first. The chain runner executes steps strictly ascending.
//
// These are the write engine's own contracts, living with their consumer
// in runtime/harness. Step numbers are impl-shaped execution order, not
// protocol ADT.
type stepID int

// Step 2 (StepDedupe) was retired along with the message-dedupe machinery;
// step 6 was retired when routing policy was judged a substrate leak and moved
// into the human membrane. Both ordinals are
// intentionally left as gaps so the remaining step numbers stay stable.
const (
	StepCallerAuth       stepID = 0
	StepEnvelopeShape    stepID = 1
	StepNormalize        stepID = 3
	StepSenderConsistent stepID = 4
	StepTypeRegistered   stepID = 5
	StepKindAndAudience  stepID = 7
	StepResponsePairing  stepID = 8
	StepReceiverGate     stepID = 9
	StepEngineAppend     stepID = 10
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

// WriteResult is the outcome of a full Chain.Write invocation.
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

// Pen is the substrate's opaque write capability — the ONLY thing an actor (or
// any writer) ever holds. A Pen is welded to one identity at mint time: every
// Write it commits carries that identity, and the holder cannot change
// it. This is the substrate's first syscall (write truth); identity rides each
// write the way a UID rides each Linux syscall. boundPen satisfies it; so does
// the relay-only proxy pen a remote (out-of-process) cell uses to emit over the
// wire — the proxy carries no local truth and observes the same verdict, so a
// remote cell's pen is indistinguishable from a local one.
type Pen interface {
	Write(ctx context.Context, env *message.Envelope) (WriteResult, error)
}

// Minter is the mint machine — the runtime's ONE outward face for producing pens. The
// platform holds the Minter (never the bare chain) and Mints a welded Pen at
// each admission point (Spawn / attach / system closure). Minting any identity
// is the highest capability in the system, so archtest confines harness.Minter
// type references to the platform tree.
//
// A minter mints, and that is all it does. The admitted write is not on it and
// is not reachable from it: they are two capabilities that happen to drive the
// same chain, and no type in this package holds both. Minter used to embed the
// admitted seam, which handed all three minting consumers a write none of them
// calls.
type Minter interface {
	// MintAuthority welds a live authority onto the chain. The returned Pen
	// re-runs that authority's one complete verdict on every Write, so the
	// same shell serves a local body for its whole term and a remote ingress
	// for one operation.
	MintAuthority(capauth.Authority, actor.Kind) Pen
}

// AdmittedWriter writes as an identity whose collaboration admission the caller
// has ALREADY completed. Its one source boundary is timer fire, where the author
// verdict is the fire gate itself — the caller needs that verdict's result in
// hand (a dead author's row is annihilated, not retried), so the pen must not
// re-reach it.
//
// It deliberately mints nothing. A pen would be a capability that outlives the
// verdict behind it, indistinguishable by type from one that re-judges on every
// write, and holding it across the moment would write as an identity that has
// since ended. What exists here is one write, not a writer.
type AdmittedWriter interface {
	WriteAdmitted(context.Context, storespec.IdentityAdmission, *message.Envelope) (WriteResult, error)
}
