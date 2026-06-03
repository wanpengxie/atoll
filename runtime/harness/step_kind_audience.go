package harness

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// defaultRequestTTLMs is the substrate-global fallback closure deadline applied
// to a request whose caller did not stamp expires_at. The deadline is a
// REQUEST-CONTRACT property the caller owns (it knows the receiver's latency
// from its capability catalog); this uniform fallback only guarantees every
// request has a bounded closure — it is NOT a per-receiver-kind or per-type
// policy (that was a type_registry-era leak: deadline-by-actor.kind).
const defaultRequestTTLMs int64 = 24 * 60 * 60 * 1000

// stepKindAndAudience implements proto-layer1 §2.6 Kind+Audience Validate — the
// substrate-essential STRUCTURE checks, with NO business-type vocabulary:
//
//   - core / reserved-namespace types: kind must match their kernel-defined rule
//   - kind=request / kind=response: audience cardinality exactly-one
//   - audience target must be a registered ACTIVE actor (pure actor addressing)
//   - a request without expires_at gets the global fallback closure deadline
//
// Business types carry NO substrate kind / handler constraint: their kind is a
// kernel closed-set value (validated at envelope-shape), and which kind is
// meaningful for "xhs.publish" — and which actor handles it — is the receiving
// actor's contract + the caller's catalog, not the substrate's. There is no
// type_registry lookup and no type→handler routing.
type stepKindAndAudience struct {
	deps Deps
}

func newStepKindAndAudience(d Deps) step { return &stepKindAndAudience{deps: d} }

func (s *stepKindAndAudience) ID() stepID { return StepKindAndAudience }

func (s *stepKindAndAudience) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	// (1) core / reserved-namespace type→kind rules — kernel's OWN vocabulary.
	if rule, ok := message.LookupCoreType(env.Type); ok {
		if !rule.AllowOverride && env.Kind != rule.DefaultKind {
			return outcome{
				RejectReason: HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("core type %s allows only kind=%s", env.Type, rule.DefaultKind),
			}, nil
		}
	} else if _, reserved := reservedBootstrapTypeSet[env.Type]; reserved {
		if env.Kind != message.KindEvent {
			return outcome{
				RejectReason: HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("reserved system type %s allows only kind=event", env.Type),
			}, nil
		}
	} else if rule, reserved := reservedActorTypeSet[env.Type]; reserved {
		if !kindAllowed(rule.AllowedKinds, env.Kind) {
			return outcome{
				RejectReason: HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("reserved actor type %s does not allow kind=%s", env.Type, env.Kind),
			}, nil
		}
	}
	// (business type: no substrate kind rule — falls through.)

	// (2) audience emptiness — single closure validation centre. The substrate
	//     does not author routing; a named audience must arrive from ingress/domain.
	if len(env.Audience) == 0 {
		return outcome{RejectReason: HarnessAudienceEmpty, Detail: "envelope.audience empty"}, nil
	}

	// (3) response — exactly-one concrete receiver.
	if env.Kind == message.KindResponse {
		if len(env.Audience) != 1 || env.Audience[0] == "" {
			return outcome{RejectReason: HarnessResponseAudienceInvalid, Detail: "kind=response requires audience cardinality 1"}, nil
		}
		return outcome{}, nil
	}

	// kind=event — no cardinality constraint beyond non-empty.
	if env.Kind != message.KindRequest {
		return outcome{}, nil
	}

	// (4) request — exactly-one concrete, ACTIVE receiver. Pure actor addressing:
	//     the caller addressed the actor it resolved (no type→handler routing).
	if len(env.Audience) != 1 || env.Audience[0] == "" {
		return outcome{RejectReason: HarnessRequestAudienceInvalid, Detail: "kind=request requires audience=[<concrete-actor>]"}, nil
	}
	target := actor.ActorID(env.Audience[0])
	rec, ok, err := s.deps.ActorRegistry.Lookup(ctx, target)
	if err != nil {
		return outcome{}, err
	}
	if !ok || !rec.IsActive() {
		return outcome{RejectReason: HarnessAudienceMemberNotActive, Detail: fmt.Sprintf("audience actor %q not active in registry", target)}, nil
	}

	// (5) request closure deadline — caller-supplied; uniform fallback when absent.
	if env.ExpiresAt == nil {
		deadline := s.deps.NowMs() + defaultRequestTTLMs
		env.ExpiresAt = &deadline
	}
	return outcome{}, nil
}

func kindAllowed(allowed []message.Kind, want message.Kind) bool {
	for _, k := range allowed {
		if k == want {
			return true
		}
	}
	return false
}
