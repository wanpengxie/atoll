package harness

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
)

type stepAudienceResolve struct{ deps Deps }

func newStepAudienceResolve(d Deps) step  { return &stepAudienceResolve{deps: d} }
func (s *stepAudienceResolve) ID() stepID { return StepAudienceResolve }

func (s *stepAudienceResolve) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	if len(env.Audience) != 0 || (env.Kind != message.KindRequest && env.Kind != message.KindEvent) || s.deps.ResolveAudience == nil {
		return outcome{}, nil
	}
	if err := s.deps.ResolveAudience(ctx, env); err != nil {
		return outcome{}, err
	}
	return outcome{}, nil
}
