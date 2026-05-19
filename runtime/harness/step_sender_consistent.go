package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepSenderConsistent implements L1 §10.2 step 3:
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

func newStepSenderConsistent(d Deps) khar.Step {
	return &stepSenderConsistent{deps: d}
}

func (s *stepSenderConsistent) ID() khar.StepID { return khar.StepSenderConsistent }

func (s *stepSenderConsistent) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	caller := CallerFromCtx(ctx)
	if caller.ActorID == "" {
		// stepCallerAuth should already have rejected; defensive.
		return khar.Outcome{
			RejectReason: message.HarnessAuthFailed,
			Detail:       "harness: caller missing at step 3",
		}, nil
	}
	if env.Sender.ID != caller.ActorID {
		return khar.Outcome{
			RejectReason: message.HarnessSenderMismatch,
			Detail: fmt.Sprintf("envelope.sender.id=%q does not match caller=%q",
				env.Sender.ID, caller.ActorID),
		}, nil
	}

	rec, ok, err := s.deps.ActorRegistry.Lookup(ctx, env.Sender.ID)
	if err != nil {
		return khar.Outcome{}, fmt.Errorf("harness: actor lookup: %w", err)
	}
	if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q not in actor_registry", env.Sender.ID),
		}, nil
	}
	if rec.DeregisteredAt != 0 && env.Sender.ID != actor.SystemActorID {
		return khar.Outcome{
			RejectReason: message.HarnessSenderDeregistered,
			Detail:       fmt.Sprintf("sender %q deregistered_at=%d", env.Sender.ID, rec.DeregisteredAt),
		}, nil
	}

	providedKind := env.Sender.Kind
	if providedKind != "" && providedKind != rec.Kind {
		if !caller.AllowProvidedSenderKind {
			// Untrusted edge — caller fabricated a kind. Reject hard.
			return khar.Outcome{
				RejectReason: message.HarnessSenderKindMismatch,
				Detail: fmt.Sprintf("envelope.sender.kind=%s does not match actor_registry=%s",
					providedKind, rec.Kind),
			}, nil
		}
		// Trusted edge but still tampered — reject hard. The "trusted"
		// flag only means the harness will tolerate a CORRECT pre-fill,
		// not a wrong one.
		return khar.Outcome{
			RejectReason: message.HarnessSenderKindMismatch,
			Detail: fmt.Sprintf("envelope.sender.kind=%s mismatched registry=%s",
				providedKind, rec.Kind),
		}, nil
	}
	// Forced overwrite — the registry is the truth.
	env.Sender.Kind = rec.Kind

	if errors.Is(ctx.Err(), context.Canceled) {
		return khar.Outcome{}, ctx.Err()
	}
	return khar.Outcome{}, nil
}
