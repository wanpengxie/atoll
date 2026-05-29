package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepAudienceResolve is the resolve half of the audience
// resolve→validate pipeline (audience-resolution-mechanism-fix.md §1).
//
// Audience handling is an ordered pipeline `resolve(intent) →
// validate(named)`. Both halves must live in the same layer / process,
// resolution first, in the one location every ingress shares (the
// harness). This step performs ONLY the resolve half:
//
//	if audience is empty AND sender.kind == human:
//	    fill the channel's declared default_rule (deps.DefaultAudience)
//	    still empty → leave empty (downstream validation rejects as-is)
//	else: no-op passthrough
//
// Gate on sender.kind == human is deliberate (§1 推论 5): humans express
// an *intent* (an empty audience means "route me to the channel's
// default host"), while agent / tool / system *make* routing decisions —
// their empty audience is a bug we must not paper over, so they fall
// straight through to StepKindAndAudience's reject.
//
// No validation lives here: cardinality, active-actor, and handler-match
// checks all stay in StepKindAndAudience (single validation centre — no
// duplication). This step runs after StepSenderConsistent (sender.kind
// is settled from the registry) and before StepKindAndAudience (so the
// validator sees the resolved audience).
type stepAudienceResolve struct {
	deps Deps
}

func newStepAudienceResolve(d Deps) khar.Step { return &stepAudienceResolve{deps: d} }

func (s *stepAudienceResolve) ID() khar.StepID { return khar.StepAudienceResolve }

func (s *stepAudienceResolve) Run(_ context.Context, env *message.Envelope) (khar.Outcome, error) {
	if env == nil {
		return khar.Outcome{}, nil
	}
	// Non-human senders and already-named audiences are passthrough.
	if len(env.Audience) != 0 || env.Sender.Kind != actor.KindHuman {
		return khar.Outcome{}, nil
	}
	if s.deps.DefaultAudience == nil {
		return khar.Outcome{}, nil
	}
	def := s.deps.DefaultAudience(env.ChannelID)
	if len(def) == 0 {
		// No declared default → leave empty; StepKindAndAudience rejects
		// it with harness_audience_empty (reason unchanged).
		return khar.Outcome{}, nil
	}
	resolved := make(message.Audience, 0, len(def))
	for _, id := range def {
		resolved = append(resolved, id)
	}
	env.Audience = resolved
	return khar.Outcome{}, nil
}
