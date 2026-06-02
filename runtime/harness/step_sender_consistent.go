package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
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

func newStepSenderConsistent(d Deps) Step {
	return &stepSenderConsistent{deps: d}
}

func (s *stepSenderConsistent) ID() StepID { return StepSenderConsistent }

func (s *stepSenderConsistent) Run(ctx context.Context, env *message.Envelope) (Outcome, error) {
	caller := CallerFromCtx(ctx)
	if caller.ActorID == "" {
		// stepCallerAuth should already have rejected; defensive.
		return Outcome{
			RejectReason: HarnessEngineACLDenied,
			Detail:       "harness: caller missing at sender-consistent step",
		}, nil
	}
	if env.Sender.ID != caller.ActorID {
		return Outcome{
			RejectReason: HarnessSenderMismatch,
			Detail: fmt.Sprintf("envelope.sender.id=%q does not match caller=%q",
				env.Sender.ID, caller.ActorID),
		}, nil
	}

	rec, ok, err := s.deps.ActorRegistry.Lookup(ctx, env.Sender.ID)
	if err != nil {
		return Outcome{}, fmt.Errorf("harness: actor lookup: %w", err)
	}
	if !ok {
		return Outcome{
			RejectReason: HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q not in actor_registry", env.Sender.ID),
		}, nil
	}
	if rec.DeregisteredAt != 0 && env.Sender.ID != actor.SystemActorID {
		return Outcome{
			RejectReason: HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q deregistered_at=%d", env.Sender.ID, rec.DeregisteredAt),
		}, nil
	}

	providedKind := env.Sender.Kind
	if providedKind != "" && providedKind != rec.Kind {
		if !caller.AllowProvidedSenderKind {
			// Untrusted edge — caller fabricated a kind. Reject hard.
			return Outcome{
				RejectReason: HarnessSenderKindMismatch,
				Detail: fmt.Sprintf("envelope.sender.kind=%s does not match actor_registry=%s",
					providedKind, rec.Kind),
			}, nil
		}
		// Trusted edge but still tampered — reject hard. The "trusted"
		// flag only means the harness will tolerate a CORRECT pre-fill,
		// not a wrong one.
		return Outcome{
			RejectReason: HarnessSenderKindMismatch,
			Detail: fmt.Sprintf("envelope.sender.kind=%s mismatched registry=%s",
				providedKind, rec.Kind),
		}, nil
	}
	// Forced overwrite — the registry is the truth.
	env.Sender.Kind = rec.Kind

	if errors.Is(ctx.Err(), context.Canceled) {
		return Outcome{}, ctx.Err()
	}
	return Outcome{}, nil
}
