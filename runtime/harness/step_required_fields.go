package harness

import (
	"context"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepRequiredFields implements L1 §10.2 step 2 — envelope L0 I1 + I7
// required fields, plus the One Law "kind=response → parent_id non-null"
// extra-strong constraint.
type stepRequiredFields struct{}

func newStepRequiredFields(_ Deps) khar.Step { return &stepRequiredFields{} }

func (s *stepRequiredFields) ID() khar.StepID { return khar.StepRequiredFields }

func (s *stepRequiredFields) Run(_ context.Context, env *message.Envelope) (khar.Outcome, error) {
	switch {
	case env.ID == "":
		return rejectMissing("envelope.id required"), nil
	case env.ChannelID == "":
		return rejectMissing("envelope.channel_id required"), nil
	case env.Type == "":
		return rejectMissing("envelope.type required"), nil
	case env.Sender.ID == "":
		return rejectMissing("envelope.sender.id required"), nil
	case env.TS == 0:
		return rejectMissing("envelope.ts required"), nil
	case len(env.Payload) == 0:
		return rejectMissing("envelope.payload required"), nil
	case env.Visibility == "":
		return rejectMissing("envelope.visibility required"), nil
	case env.Audience == nil:
		return rejectMissing("envelope.audience required"), nil
	}

	// I7: kind must be in the closed set.
	if env.Kind == "" {
		return khar.Outcome{
			RejectReason: message.HarnessKindInvalid,
			Detail:       "envelope.kind empty (business type must declare kind)",
		}, nil
	}
	switch env.Kind {
	case message.KindEvent, message.KindRequest, message.KindResponse:
	default:
		return khar.Outcome{
			RejectReason: message.HarnessKindInvalid,
			Detail:       "envelope.kind not in {event, request, response}",
		}, nil
	}

	if env.Kind == message.KindResponse && env.ParentID == "" {
		return khar.Outcome{
			RejectReason: message.HarnessResponseMissingParentID,
			Detail:       "kind=response requires non-empty parent_id",
		}, nil
	}

	return khar.Outcome{}, nil
}

func rejectMissing(detail string) khar.Outcome {
	return khar.Outcome{
		RejectReason: message.HarnessMissingRequiredField,
		Detail:       detail,
	}
}
