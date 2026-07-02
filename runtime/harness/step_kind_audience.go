package harness

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/message"
)

// defaultRequestTTLMs is the substrate-global fallback closure deadline applied
// to a request whose caller did not stamp expires_at. The deadline is a
// REQUEST-CONTRACT property the caller owns (it knows the receiver's latency
// from its capability catalog); this uniform fallback only guarantees every
// request has a bounded closure — it is NOT a per-receiver-kind or per-type
// policy (deadline-by-actor.kind was a per-type-policy leak, now removed).
const defaultRequestTTLMs int64 = 24 * 60 * 60 * 1000

// stepKindAndAudience performs Kind+Audience validation — the
// substrate-essential STRUCTURE checks, with NO business-type vocabulary:
//
//   - core / reserved-namespace types: kind must match their kernel-defined rule
//   - kind=request / kind=response: audience cardinality exactly-one
//   - a request without expires_at gets the global fallback closure deadline
//
// Receiver reachability/liveness is deliberately NOT checked here: it is not a
// property of the writer (a write harness validates truth about the SENDER), it
// is racy at write time (TOCTOU), and its only authority is actorrt's live
// embodiments — which this engine is structurally decoupled from. It is
// resolved at the delivery seam (Deliver→NotHosted→closure materialises
// receiver_unavailable) instead of at write time.
//
// Business types carry NO substrate kind / handler constraint: their kind is a
// kernel closed-set value (validated at envelope-shape), and which kind is
// meaningful for "xhs.publish" — and which actor handles it — is the receiving
// actor's contract + the caller's catalog, not the substrate's. There is no
// per-type registry lookup and no type→handler routing.
type stepKindAndAudience struct {
	deps Deps
}

func newStepKindAndAudience(d Deps) step { return &stepKindAndAudience{deps: d} }

func (s *stepKindAndAudience) ID() stepID { return StepKindAndAudience }

func (s *stepKindAndAudience) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	// (1) core / reserved-namespace type→kind rules — kernel's OWN vocabulary.
	// This AllowOverride=false branch is the LIVE enforcer of
	// CoreTypeRule.DefaultKind as a constraint (not a fill). It currently has NO
	// subject — both live core types are AllowOverride=true — but is kept as
	// additive-ready machinery: a future non-overridable core
	// type reactivates it. The reserved-bootstrap branch below is its live sibling.
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
	}
	// (business type AND actor.* introspection: no substrate kind rule — falls
	// through. actor.* is type-agnostic to the substrate; its req/resp shape is
	// an upper-layer convention, not a substrate gate.)

	// (2) audience emptiness — single closure validation centre. The substrate
	//     does not author routing; a named audience must arrive from the caller.
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

	// (4) request — exactly-one concrete receiver (STRUCTURE only). The caller
	//     addressed the actor it resolved (no type→handler routing). Whether that
	//     receiver is reachable/live is NOT asserted here — that is the delivery
	//     seam's job (Deliver→NotHosted→closure materialises receiver_unavailable),
	//     not the writer's. Do not re-add a registry liveness lookup.
	if len(env.Audience) != 1 || env.Audience[0] == "" {
		return outcome{RejectReason: HarnessRequestAudienceInvalid, Detail: "kind=request requires audience=[<concrete-actor>]"}, nil
	}

	// (5) request closure deadline — caller-supplied; uniform fallback when absent.
	if env.ExpiresAt == nil {
		deadline := s.deps.NowMs() + defaultRequestTTLMs
		env.ExpiresAt = &deadline
	}
	return outcome{}, nil
}
