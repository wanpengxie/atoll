package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	defaultAgentMaxPendingMs  int64 = 24 * 60 * 60 * 1000
	defaultSystemMaxPendingMs int64 = 60 * 60 * 1000
	defaultActorMaxPendingMs  int64 = 30 * 1000
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
		view            TypeView
		isCore          bool
		isReservedActor bool
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
	} else if _, reserved := reservedBootstrapTypeSet[env.Type]; reserved {
		isCore = true
		if env.Kind != message.KindEvent {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("reserved system type %s allows only kind=event", env.Type),
			}, nil
		}
	} else if rule, reserved := reservedActorTypeSet[env.Type]; reserved {
		isCore = true
		isReservedActor = true
		if !kindAllowed(rule.AllowedKinds, env.Kind) {
			return khar.Outcome{
				RejectReason: message.HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("reserved actor type %s does not allow kind=%s", env.Type, env.Kind),
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

	// Audience emptiness is the single validation centre for the
	// resolve→validate pipeline. The pre-resolution reject was moved here
	// from StepEnvelopeShape so it runs over the *resolved* audience:
	// StepAudienceResolve (which ran just before this step) fills the
	// channel default for human senders, so an empty audience reaching
	// here means either a non-human sender (whose empty audience is a bug
	// we surface, not paper over) or a channel with no declared default.
	// Either way the reason is unchanged — harness_audience_empty.
	if len(env.Audience) == 0 {
		return khar.Outcome{
			RejectReason: message.HarnessAudienceEmpty,
			Detail:       "envelope.audience empty",
		}, nil
	}

	// response — audience exactly-one concrete receiver. This cardinality
	// check moved here from StepEnvelopeShape so it runs over the
	// resolved audience. Validation lives in one place.
	if env.Kind == message.KindResponse {
		if len(env.Audience) != 1 || env.Audience[0] == "" {
			return khar.Outcome{
				RejectReason: message.HarnessResponseAudienceInvalid,
				Detail:       "kind=response requires audience cardinality 1",
			}, nil
		}
		return khar.Outcome{}, nil
	}

	if env.Kind != message.KindRequest {
		// kind=event — no audience cardinality constraint beyond the
		// non-empty + wildcard ban already enforced above / in
		// StepEnvelopeShape.
		return khar.Outcome{}, nil
	}

	// request — audience exactly-one concrete receiver. Step 2 has
	// already rejected wildcard audience entries; StepAudienceResolve has
	// filled the channel default for human senders. len>1 is an explicit
	// fan-out request → harness_request_audience_invalid.
	if len(env.Audience) != 1 || env.Audience[0] == "" {
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
		if isReservedActor {
			deadline := s.deps.NowMs() + defaultActorMaxPendingMs
			env.ExpiresAt = &deadline
			return khar.Outcome{}, nil
		}
		out, err := s.defaultExpiresAt(env, rec.Kind, view, !isCore)
		if err != nil {
			return khar.Outcome{}, err
		}
		if !out.Continue() {
			return out, nil
		}
	}
	return khar.Outcome{}, nil
}

// errTypeRegistryMaxPendingMissing is returned (as a non-protocol Go
// error, NOT a closed-set harness reject) when the type_registry row
// for a tool receiver is missing the max_pending_ms field. Install
// already enforces this invariant (InstallAdapterTimeoutMissing); the
// harness fails loudly via the runtime error path because the
// condition reflects internal registry corruption, not a caller-visible
// protocol violation.
var errTypeRegistryMaxPendingMissing = errors.New("harness: type_registry row missing max_pending_ms for tool receiver (install invariant violated)")

func (s *stepKindAndAudience) defaultExpiresAt(
	env *message.Envelope,
	receiverKind actor.Kind,
	view TypeView,
	hasTypeView bool,
) (khar.Outcome, error) {
	var maxPendingMs int64
	switch receiverKind {
	case actor.KindTool:
		if !hasTypeView || view.MaxPendingMs <= 0 {
			// Type registry installed the tool handler but omitted
			// max_pending_ms — install validator should have caught this
			// (InstallAdapterTimeoutMissing). Surface as a non-protocol
			// runtime error so the failure is loud and the proto-layer1
			// §2.11.1 closed reject set stays clean.
			return khar.Outcome{}, fmt.Errorf("%w: type=%s", errTypeRegistryMaxPendingMissing, env.Type)
		}
		maxPendingMs = view.MaxPendingMs
	case actor.KindAgent:
		maxPendingMs = defaultAgentMaxPendingMs
	case actor.KindSystem:
		maxPendingMs = defaultSystemMaxPendingMs
	case actor.KindHuman:
		return khar.Outcome{}, nil
	default:
		return khar.Outcome{}, nil
	}
	deadline := s.deps.NowMs() + maxPendingMs
	env.ExpiresAt = &deadline
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
