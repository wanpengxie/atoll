package harness

import (
	"context"
	"encoding/json"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepNormalize implements L1 §10.2 step 0 — default-fill audience /
// visibility / kind / correlation_id / payload-baseline (`{}` for nil).
// The step never rejects; downstream steps run the actual validation.
type stepNormalize struct {
	deps Deps
}

func newStepNormalize(d Deps) khar.Step { return &stepNormalize{deps: d} }

func (s *stepNormalize) ID() khar.StepID { return khar.StepNormalize }

func (s *stepNormalize) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	_ = ctx
	if env == nil {
		return khar.Outcome{}, nil
	}

	// audience default → ['*'].
	if env.Audience == nil {
		env.Audience = message.Audience{message.AudienceWildcard}
	}

	// visibility default → public.
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}

	// kind default (core types only — business types must declare).
	if env.Kind == "" {
		if rule, ok := CoreTypeTable[env.Type]; ok {
			env.Kind = rule.DefaultKind
		}
	}

	// correlation_id default → envelope.id when caller did not propagate
	// trigger correlation. L1 §10.2 specifies a 3-layer fallback; the
	// trigger-context branch belongs to FIX-T3 (trigger gateway). For
	// now we collapse "no trigger context" + "no caller hint" into the
	// self-rooted fallback (correlation_id = id) so new chains stay
	// non-NULL per L1 §10.2.
	if env.CorrelationID == "" && env.ID != "" {
		env.CorrelationID = env.ID
	}

	// payload baseline. L0 §2.2 forbids `payload=null`; `payload={}`
	// is legal. Caller may omit (empty bytes); we substitute `{}` so
	// CanonicalHash + schema validators see a well-formed object.
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("{}")
	}

	// ts default — caller usually sets this, but tolerate a missing
	// value so partial test envelopes don't hit step 2 missing_required.
	if env.TS == 0 {
		env.TS = s.deps.NowMs()
	}

	return khar.Outcome{}, nil
}
