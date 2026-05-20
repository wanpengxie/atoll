package harness

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	defaultAgentMaxPendingMs  int64 = 24 * 60 * 60 * 1000
	defaultSystemMaxPendingMs int64 = 60 * 60 * 1000
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
	var (
		view   TypeView
		isCore bool
	)
	if rule, ok := message.CoreTypeTable[env.Type]; ok {
		isCore = true
		if !rule.AllowOverride && env.Kind != rule.DefaultKind {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowedForType,
				Detail: fmt.Sprintf("core type %s allows only kind=%s",
					env.Type, rule.DefaultKind),
			}, nil
		}
	} else {
		// business type — look up allowed_kinds. unknown_type was already
		// caught at step 4, so we expect a hit here.
		var ok bool
		var err error
		view, ok, err = s.deps.TypeRegistry.Lookup(ctx, env.Type)
		if err != nil {
			return khar.Outcome{}, err
		}
		if !ok {
			return khar.Outcome{
				RejectReason: message.HarnessTypeUnknown,
				Detail:       "type lookup vanished between step 4 and 5: " + env.Type,
			}, nil
		}
		if !kindAllowed(view.AllowedKinds, env.Kind) {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowedForType,
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
			RejectReason: message.HarnessAudienceMemberNotActive,
			Detail:       fmt.Sprintf("audience actor %q not active in registry", target),
		}, nil
	}

	// business type handler_actor_id check (core types have no handler).
	if !isCore && view.HandlerActorID != "" && view.HandlerActorID != target {
		return khar.Outcome{
			RejectReason: message.HarnessAudienceHandlerMismatch,
			Detail: fmt.Sprintf("audience=%q must equal handler_actor_id=%q",
				target, view.HandlerActorID),
		}, nil
	}
	if env.ExpiresAt == nil {
		if out := s.defaultExpiresAt(env, rec.Kind, view, !isCore); !out.Continue() {
			return out, nil
		}
	}
	return khar.Outcome{}, nil
}

func (s *stepKindAndAudience) defaultExpiresAt(
	env *message.Envelope,
	receiverKind actor.Kind,
	view TypeView,
	hasTypeView bool,
) khar.Outcome {
	var maxPendingMs int64
	switch receiverKind {
	case actor.KindTool:
		if !hasTypeView || view.MaxPendingMs <= 0 {
			// Type registry installed the tool handler but omitted
			// max_pending_ms — install validator should have caught this,
			// but harness fails closed using harness_schema_missing (the
			// closest match in proto-layer1 §2.11.1 for "registry config
			// the harness needs is absent").
			return khar.Outcome{
				RejectReason: message.HarnessSchemaMissing,
				Detail:       "tool receiver requires type_registry.max_pending_ms to default expires_at",
			}
		}
		maxPendingMs = view.MaxPendingMs
	case actor.KindAgent:
		maxPendingMs = defaultAgentMaxPendingMs
	case actor.KindSystem:
		maxPendingMs = defaultSystemMaxPendingMs
	case actor.KindHuman:
		return khar.Outcome{}
	default:
		return khar.Outcome{}
	}
	deadline := s.deps.NowMs() + maxPendingMs
	env.ExpiresAt = &deadline
	return khar.Outcome{}
}

func kindAllowed(allowed []message.Kind, want message.Kind) bool {
	for _, k := range allowed {
		if k == want {
			return true
		}
	}
	return false
}
