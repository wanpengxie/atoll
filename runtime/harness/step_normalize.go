package harness

import (
	"context"
	"encoding/json"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepNormalize implements proto-layer1 §2.4 step 4 — default-fill
// audience / visibility / kind / correlation_id / payload baseline, plus
// the §2.4 time-relation guard run AFTER the default-fill phase:
//
//   - not_before defaults to ts when absent; not_before >= ts is required.
//   - expires_at, when present, must be > ts and > not_before.
//
// Violations reject with harness_time_invalid. All other normalize
// branches are pure data-fill and never reject.
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
		if rule, ok := message.CoreTypeTable[env.Type]; ok {
			env.Kind = rule.DefaultKind
		}
	}

	// correlation_id default — same self-rooted fallback the previous
	// round used. Trigger-context branch belongs to FIX-T3 (trigger
	// gateway) and is not affected by this step.
	if env.CorrelationID == "" && env.ID != "" {
		env.CorrelationID = env.ID
	}

	// payload baseline. proto-layer0 §1.1 admits a missing payload for
	// kind=event; store and adapter handlers still expect a JSON object, so
	// normalize substitutes `{}` on the wire before reaching them. Note:
	// StepDedupe ran BEFORE this substitution so canonical_hash sees the
	// raw sender-provided payload (proto-layer1 §2.3).
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("{}")
	}

	// ts default — caller usually sets this, but tolerate a missing
	// value so partial test envelopes don't trip downstream checks.
	if env.TS == 0 {
		env.TS = s.deps.NowMs()
	}

	// not_before default — proto-layer0 §4.5: unset == ts.
	if env.NotBefore == nil {
		nb := env.TS
		env.NotBefore = &nb
	}

	// time-relation guard — proto-layer1 §2.4 / proto-layer0 §4.5.
	if env.NotBefore != nil && *env.NotBefore < env.TS {
		return khar.Outcome{
			RejectReason: message.HarnessTimeInvalid,
			Detail:       "envelope.not_before < envelope.ts",
		}, nil
	}
	if env.ExpiresAt != nil {
		if *env.ExpiresAt <= env.TS {
			return khar.Outcome{
				RejectReason: message.HarnessTimeInvalid,
				Detail:       "envelope.expires_at <= envelope.ts",
			}, nil
		}
		if env.NotBefore != nil && *env.ExpiresAt <= *env.NotBefore {
			return khar.Outcome{
				RejectReason: message.HarnessTimeInvalid,
				Detail:       "envelope.expires_at <= envelope.not_before",
			}, nil
		}
	}

	return khar.Outcome{}, nil
}
