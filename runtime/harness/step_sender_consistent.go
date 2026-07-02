package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// stepSenderConsistent implements the Sender Validate step (StepSenderConsistent
// in the impl ordering; proto-layer1 §2.6 Sender Validate):
//
//   - envelope.sender.id == caller.actor_id (sender_mismatch otherwise)
//   - envelope.sender.kind matches the pen-welded caller.kind; caller-provided
//     kind that conflicts → sender_kind_mismatch. Always OVERWRITE the envelope
//     with the welded truth so downstream callers see the canonical value
//     (forced-overwrite per L1 §10.2.1).
//
// There is deliberately NO actor_registry lookup here (incarnation-dynamics
// build-spec §3.2 / §1.4): identity is pen-welded (sender.id above) and
// liveness is gated one layer up by livePen.IsLive() (platform/internal/
// link/livepen.go) on every write, before this chain even runs. A registry
// name-list check here would be a second, redundant authority over the same
// "is this a real, live writer" question — this step trusts the pen.
type stepSenderConsistent struct {
	deps Deps
}

func newStepSenderConsistent(d Deps) step {
	return &stepSenderConsistent{deps: d}
}

func (s *stepSenderConsistent) ID() stepID { return StepSenderConsistent }

func (s *stepSenderConsistent) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	c := callerFromCtx(ctx)
	if c.actorID == "" {
		// stepCallerAuth should already have rejected; defensive.
		return outcome{
			RejectReason: HarnessEngineACLDenied,
			Detail:       "harness: caller missing at sender-consistent step",
		}, nil
	}
	// With the boundPen welding the caller's actorID into env.Sender.ID before
	// the chain runs, this comparison is effectively tautological — but it is retained as
	// the chain's own self-consistency assertion (the chain does not depend on
	// the pen always welding; it self-validates completely).
	if env.Sender.ID != c.actorID {
		return outcome{
			RejectReason: HarnessSenderMismatch,
			Detail: fmt.Sprintf("envelope.sender.id=%q does not match caller=%q",
				env.Sender.ID, c.actorID),
		}, nil
	}

	// Welded-kind closed-set gate (incarnation-dynamics-build-spec §3.2 point 5,
	// the "mint 前统一收口点"): with the registry lookup gone, this is the ONE
	// chokepoint every pen flows through before its kind is stamped into a
	// durable row — the wire path is already guarded at attach (accept.go's
	// ParseKind over declarations), but in-process pens (Home.Spawn's kind
	// param, a registry Constructor's decl.Kind) reach here unvalidated, and
	// the store's Append does not re-check kind (only the READ scan does), so
	// without this gate an out-of-set kind would land as a poisoned row that
	// only explodes on a later read. An out-of-set WELDED kind is an assembly/
	// programmer error (Mint welded garbage), not a caller envelope fault — so
	// it is a hard engine error, not a closed-set reject.
	if _, ok := actor.ParseKind(string(c.kind)); !ok {
		return outcome{}, fmt.Errorf("harness: welded sender kind %q out of closed set (assembly bug: Mint welded an invalid kind)", c.kind)
	}

	providedKind := env.Sender.Kind
	if providedKind != "" && providedKind != c.kind {
		// A caller-provided kind that contradicts the pen-welded truth is a
		// misreport of identity (A3 真实). The weld is the single identity
		// truth; reject hard. There is no "trusted transport may self-assert
		// its kind" mode — that axis is a downstream transport distinction
		// the weld-as-truth axiom collapses.
		return outcome{
			RejectReason: HarnessSenderKindMismatch,
			Detail: fmt.Sprintf("envelope.sender.kind=%s does not match welded kind=%s",
				providedKind, c.kind),
		}, nil
	}
	// Forced overwrite — the pen-welded kind is the truth.
	env.Sender.Kind = c.kind

	if errors.Is(ctx.Err(), context.Canceled) {
		return outcome{}, ctx.Err()
	}
	return outcome{}, nil
}
