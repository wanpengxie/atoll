package harness

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepKindAndAudience implements L1 §10.2 step 5:
//
//   - core types: kind must equal default_kind unless AllowOverride
//   - business types: kind ∈ registry.types[type].allowed_kinds
//   - kind=request requires audience exactly-one concrete receiver
//   - audience target must be a registered active actor
//   - when registry has handler_actor_id, explicit audience must match
type stepKindAndAudience struct {
	deps Deps
}

func newStepKindAndAudience(d Deps) khar.Step { return &stepKindAndAudience{deps: d} }

func (s *stepKindAndAudience) ID() khar.StepID { return khar.StepKindAndAudience }

func (s *stepKindAndAudience) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	if rule, isCore := message.CoreTypeTable[env.Type]; isCore {
		if !rule.AllowOverride && env.Kind != rule.DefaultKind {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowed,
				Detail: fmt.Sprintf("core type %s allows only kind=%s",
					env.Type, rule.DefaultKind),
			}, nil
		}
	} else {
		// business type — look up allowed_kinds. unknown_type was already
		// caught at step 4, so we expect a hit here.
		view, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type)
		if err != nil {
			return khar.Outcome{}, err
		}
		if !ok {
			return khar.Outcome{
				RejectReason: message.HarnessUnknownType,
				Detail:       "type lookup vanished between step 4 and 5: " + env.Type,
			}, nil
		}
		if !kindAllowed(view.AllowedKinds, env.Kind) {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowed,
				Detail:       fmt.Sprintf("kind=%s not allowed for type=%s", env.Kind, env.Type),
			}, nil
		}
	}

	if env.Kind != message.KindRequest {
		return khar.Outcome{}, nil
	}

	// request — audience exactly-one concrete receiver.
	if len(env.Audience) != 1 || env.Audience[0] == "" || env.Audience[0] == "*" {
		return khar.Outcome{
			RejectReason: message.HarnessRequestAudienceInvalid,
			Detail:       "kind=request requires audience=[<concrete-actor>]",
		}, nil
	}
	target := actor.ActorID(env.Audience[0])

	rec, ok, err := s.deps.ActorRegistry.Lookup(ctx, target)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok || !rec.IsActive() {
		return khar.Outcome{
			RejectReason: message.HarnessAudienceActorNotRegistered,
			Detail:       fmt.Sprintf("audience actor %q not active in registry", target),
		}, nil
	}

	// business type handler_actor_id check (core types have no handler).
	if _, isCore := message.CoreTypeTable[env.Type]; !isCore {
		view, _, _ := s.deps.TypeRegistry.Lookup(ctx, env.Type)
		if view.HandlerActorID != "" && view.HandlerActorID != target {
			return khar.Outcome{
				RejectReason: message.HarnessAudienceHandlerMismatch,
				Detail: fmt.Sprintf("audience=%q must equal handler_actor_id=%q",
					target, view.HandlerActorID),
			}, nil
		}
	}
	return khar.Outcome{}, nil
}

func kindAllowed(allowed []message.Kind, want message.Kind) bool {
	for _, k := range allowed {
		if k == want {
			return true
		}
	}
	return false
}
