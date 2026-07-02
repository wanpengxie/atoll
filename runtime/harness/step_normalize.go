package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/message"
)

// stepNormalize default-fills audience / visibility / kind / correlation_id /
// payload baseline, plus a time-relation guard run AFTER the default-fill
// phase:
//
//   - expires_at, when present, must be > ts.
//
// Violations reject with harness_time_invalid. All other normalize
// branches are pure data-fill and never reject.
type stepNormalize struct {
	deps Deps
}

func newStepNormalize(d Deps) step { return &stepNormalize{deps: d} }

func (s *stepNormalize) ID() stepID { return StepNormalize }

func (s *stepNormalize) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	_ = ctx
	if env == nil {
		return outcome{}, nil
	}

	// ts_received is engine-owned and set ONCE at the canonical append sink
	// — Chain.Write right before Log.Append — so normalize does not touch it;
	// any caller-supplied value is overwritten there.

	// audience is caller-owned. nil ≠ empty for downstream step 5 audience
	// cardinality check: nil treated as "empty" → harness_audience_empty.

	// visibility default → public.
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}

	// There is deliberately NO "default kind" fill here. stepEnvelopeShape
	// (runs BEFORE normalize, chain.go) rejects env.Kind == "" with
	// field_missing and short-circuits, so a kind-fill branch would be dead
	// code. kind is sender-required; the core-type table's kind field is NOT
	// a fill-default but a CONSTRAINT enforced in stepKindAndAudience (a
	// non-overridable core type's kind must equal its canonical kind). See
	// CoreTypeRule + stepKindAndAudience.

	// correlation_id default: a self-rooted fallback — an envelope with no
	// correlation_id roots a new correlation tree at its own id.
	if env.CorrelationID == "" && env.ID != "" {
		env.CorrelationID = env.ID
	}

	// payload baseline: the canonical wire form is a JSON object. normalize
	// substitutes `{}` when the caller omits payload so every appended row
	// carries a valid JSON value.
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("{}")
	}

	// kind=response (provisional + final) has no independent expires_at
	// semantics: a provisional response is not part of closure (no SLA)
	// and a final response is itself the terminal (no separate deadline).
	// Normalize clears whatever the caller may have plumbed so the
	// time-relation guard below + any downstream closure reader never act
	// on a meaningless response deadline.
	if env.Kind == message.KindResponse {
		env.ExpiresAt = nil
	}

	// No ts default: TS is a caller-set CONSTRAINT, not a normalize fill —
	// step_envelope_shape already rejects env.TS == 0 ("envelope.ts required")
	// BEFORE this step runs, so a fill here would be dead. Symmetric with
	// kind, which is also a pure constraint rather than a normalize default.

	// time-relation guard: expires_at, when present, must be strictly after
	// ts.
	if env.ExpiresAt != nil && *env.ExpiresAt <= env.TS {
		return outcome{
			RejectReason: HarnessTimeInvalid,
			Detail:       "envelope.expires_at <= envelope.ts",
		}, nil
	}

	return outcome{}, nil
}
