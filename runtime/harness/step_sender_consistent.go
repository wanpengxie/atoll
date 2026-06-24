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
//   - actor_registry resolves sender.id (sender_deregistered when absent)
//   - actor_registry.deregistered_at IS NULL (sender_deregistered when
//     the actor was soft-deleted; system is exempt per spec)
//   - envelope.sender.kind matches actor_kind; caller-provided kind that
//     conflicts → sender_kind_mismatch. Always OVERWRITE the envelope
//     with the registry's truth so downstream callers see the canonical
//     value (forced-overwrite per L1 §10.2.1).
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
	// the chain runs, this comparison is effectively恒真 — but it is retained as
	// the chain's own self-consistency assertion (the chain does not depend on
	// the pen always welding; it self-validates completely).
	if env.Sender.ID != c.actorID {
		return outcome{
			RejectReason: HarnessSenderMismatch,
			Detail: fmt.Sprintf("envelope.sender.id=%q does not match caller=%q",
				env.Sender.ID, c.actorID),
		}, nil
	}

	rec, ok, err := s.deps.ActorRegistry.Lookup(ctx, env.Sender.ID)
	if err != nil {
		return outcome{}, fmt.Errorf("harness: actor lookup: %w", err)
	}
	if !ok {
		return outcome{
			RejectReason: HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q not in actor_registry", env.Sender.ID),
		}, nil
	}
	if rec.DeregisteredAt != 0 && env.Sender.ID != actor.SystemActorID {
		return outcome{
			RejectReason: HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q deregistered_at=%d", env.Sender.ID, rec.DeregisteredAt),
		}, nil
	}

	providedKind := env.Sender.Kind
	if providedKind != "" && providedKind != rec.Kind {
		// A caller-provided kind that contradicts the registry is a
		// misreport of identity (A3 真实). The registry is the single
		// identity truth; reject hard. There is no "trusted transport may
		// self-assert its kind" mode — that axis is a downstream transport
		// distinction the registry-as-truth axiom collapses.
		return outcome{
			RejectReason: HarnessSenderKindMismatch,
			Detail: fmt.Sprintf("envelope.sender.kind=%s does not match actor_registry=%s",
				providedKind, rec.Kind),
		}, nil
	}
	// Forced overwrite — the registry is the truth.
	env.Sender.Kind = rec.Kind

	if errors.Is(ctx.Err(), context.Canceled) {
		return outcome{}, ctx.Err()
	}
	return outcome{}, nil
}
