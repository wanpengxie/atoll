package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// stepNormalize implements proto-layer1 §2.4 step 4 — default-fill
// audience / visibility / kind / correlation_id / payload baseline, plus
// the §2.4 time-relation guard run AFTER the default-fill phase:
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

	// (ts_received is engine-owned and set ONCE at the canonical append sink
	// — Chain.Write right before Log.Append — so normalize does not touch it;
	// any caller-supplied value is overwritten there per A3.)

	// audience is now caller-owned (post wildcard removal). nil ≠ empty
	// for downstream step 5 audience cardinality check: nil treated as
	// "empty" → harness_audience_empty.

	// visibility default → public.
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}

	// NB (C7, 2026-06-11): there is deliberately NO "default kind" fill here.
	// stepEnvelopeShape (runs BEFORE normalize, chain.go) rejects env.Kind == ""
	// with field_missing and short-circuits, so any kind-fill branch is dead code
	// — the former one (LookupCoreType → DefaultKind) was removed. kind is
	// sender-required; the core-type table's kind field is NOT a fill-default but
	// a CONSTRAINT enforced in stepKindAndAudience (a non-overridable core type's
	// kind must equal its canonical kind). See CoreTypeRule + stepKindAndAudience.

	// correlation_id default: a self-rooted fallback — an envelope with no
	// correlation_id roots a new correlation tree at its own id.
	if env.CorrelationID == "" && env.ID != "" {
		env.CorrelationID = env.ID
	}

	// payload baseline: proto-layer0 §1.1 defines the canonical wire form as a
	// JSON object. normalize substitutes `{}` when the caller omits payload so
	// every appended row carries a valid JSON value.
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("{}")
	}

	// kind=response (provisional + final) has no independent expires_at
	// semantics — proto-layer0 §2.7 + §4.6: provisional response is not
	// part of closure (no SLA) and final response is itself the terminal
	// (no separate deadline). Normalize clears whatever the caller may
	// have plumbed so the time-relation guard below + any downstream
	// closure reader never act on a meaningless response deadline.
	if env.Kind == message.KindResponse {
		env.ExpiresAt = nil
	}

	// (no ts default: TS is a caller-set CONSTRAINT, not a normalize fill —
	// step_envelope_shape already rejects env.TS == 0 ("envelope.ts required")
	// BEFORE this step runs, so a fill here is dead. Symmetric with kind, which
	// became a pure constraint when its normalize default was removed.)

	// time-relation guard — proto-layer1 §2.4. expires_at, when present,
	// must be strictly after ts.
	if env.ExpiresAt != nil && *env.ExpiresAt <= env.TS {
		return outcome{
			RejectReason: HarnessTimeInvalid,
			Detail:       "envelope.expires_at <= envelope.ts",
		}, nil
	}

	return outcome{}, nil
}
