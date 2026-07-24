package harness

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type stepReceiverGate struct{ presence storespec.IdentityPresence }

func newStepReceiverGate(deps Deps) step { return stepReceiverGate{presence: deps.Presence} }
func (stepReceiverGate) ID() stepID      { return StepReceiverGate }

func (s stepReceiverGate) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	if env.Kind == message.KindRequest {
		ok, err := s.presence.IsActive(ctx, env.Audience[0])
		if err != nil {
			return outcome{}, err
		}
		if !ok {
			return outcome{RejectReason: HarnessReceiverNotMember, Detail: string(env.Audience[0])}, nil
		}
	}
	return outcome{}, nil
}
