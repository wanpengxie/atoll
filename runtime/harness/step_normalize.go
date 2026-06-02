package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/message"
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

	// Sender-provided runtime fields are not protocol content. Dedupe has
	// already compared sender-provided content, so normalize can clear
	// these before StepResponsePairing / EngineAppend materialize the
	// authoritative values.
	env.TSReceived = 0

	// audience is now caller-owned (post wildcard removal). nil ≠ empty
	// for downstream step 5 audience cardinality check: nil treated as
	// "empty" → harness_audience_empty.

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

	// kind=response (provisional + final) has no independent expires_at
	// semantics — proto-layer0 §2.7 + §4.6: provisional response is not
	// part of closure (no SLA) and final response is itself the terminal
	// (no separate deadline). Normalize clears whatever the caller may
	// have plumbed so the time-relation guard below + downstream
	// scheduler scans never act on a meaningless response deadline.
	if env.Kind == message.KindResponse {
		env.ExpiresAt = nil
	}

	// ts default — caller usually sets this, but tolerate a missing
	// value so partial test envelopes don't trip downstream checks.
	if env.TS == 0 {
		env.TS = s.deps.NowMs()
	}

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
