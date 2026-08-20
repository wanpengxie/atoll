package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/message"
)

// stepNormalize canonicalizes audience and default-fills visibility / correlation_id /
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

	// Event audience is optional but has exactly one wire/storage representation:
	// [] rather than null. Request/response nil remains empty for the downstream
	// cardinality rejection.
	if env.Kind == message.KindEvent && env.Audience == nil {
		env.Audience = message.Audience{}
	}

	// visibility default → public.
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}

	// There is deliberately NO "default kind" fill here. stepEnvelopeShape
	// (runs BEFORE normalize, chain.go) rejects env.Kind == "" with
	// field_missing and short-circuits, so a kind-fill branch would be dead
	// code. kind is sender-required and is a pure CONSTRAINT, not a normalize
	// fill. See stepEnvelopeShape + stepKindAndAudience.

	// There is deliberately NO correlation_id fill. This used to root a
	// correlation-less envelope at its own id, which reads as a fill and is
	// actually a CLAIM: "nothing caused this". It is wrong for every envelope
	// written to serve another one, and it was wrong silently — a relay that
	// had no way to state its cause got a plausible answer instead of an error,
	// and its row floated beside the request that provoked it with nothing on
	// the ledger connecting the two. Cause is now a required builder input
	// (lib/behavior), so an empty correlation here means something skipped the
	// builder; stepEnvelopeShape rejects it rather than inventing an answer.

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
