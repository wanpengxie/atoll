package harness

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type stepAuthorGate struct{ authority storespec.ActorAuthority }

func newStepAuthorGate(deps Deps) step { return stepAuthorGate{authority: deps.Authority} }
func (stepAuthorGate) ID() stepID      { return StepAuthorGate }

func (s stepAuthorGate) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	principal := callerFromCtx(ctx)
	if !principal.admitted {
		verdict, err := s.authority.CheckAuthor(ctx, storespec.AuthorStamp{
			ID: principal.actorID, BirthVersion: principal.birthVersion,
		})
		if err != nil {
			return outcome{}, err
		}
		switch verdict {
		case storespec.AuthorNotMember:
			return outcome{RejectReason: HarnessAuthorNotMember, Detail: string(principal.actorID)}, nil
		case storespec.AuthorVersionStale:
			return outcome{RejectReason: HarnessAuthorVersionStale, Detail: string(principal.actorID)}, nil
		case storespec.AuthorOK:
		default:
			return outcome{RejectReason: HarnessAuthorNotMember, Detail: string(principal.actorID)}, nil
		}
	}
	if env.Kind == message.KindRequest {
		_, ok, err := s.authority.LookupActive(ctx, env.Audience[0])
		if err != nil {
			return outcome{}, err
		}
		if !ok {
			return outcome{RejectReason: HarnessReceiverNotMember, Detail: string(env.Audience[0])}, nil
		}
	}
	return outcome{}, nil
}
