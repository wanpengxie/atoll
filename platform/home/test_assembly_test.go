package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type inertIntroductionResolver struct{}

func (inertIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
}
func (inertIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}

type emptyCompositionResolver struct{}

func (emptyCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func completeHomeTestConfig(cfg Config) Config {
	if cfg.CompositionResolver == nil {
		cfg.CompositionResolver = emptyCompositionResolver{}
	}
	if cfg.IntroductionResolver == nil {
		cfg.IntroductionResolver = inertIntroductionResolver{}
	}
	return cfg
}

// admitThroughSysOp and removeThroughSysOp keep white-box setup on the same
// serving gate as production. They add only fresh test coordinates; they do not
// expose a second mutation path.
func admitThroughSysOp(h *Home, ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if kind != actor.KindHuman {
		return "", &channelspec.OperationError{Code: channelspec.ErrCodeBadPayload, Detail: "admit creates human identities"}
	}
	result, err := SystemOps(h).Admit(ctx, channelspec.AdmitRequest{Ref: "test:admit:" + uuid.NewString(), Principal: principal})
	return result.ActorID, err
}

func removeThroughSysOp(h *Home, ctx context.Context, target actor.ActorID) error {
	_, err := SystemOps(h).Remove(ctx, channelspec.RemoveRequest{
		Ref: "test:remove:" + uuid.NewString(), Target: target, InitiatorActorID: target,
	})
	return err
}

func isChannelUnavailableForTest(err error) bool {
	var opErr *channelspec.OperationError
	return errors.As(err, &opErr) && opErr.Code == channelspec.ErrCodeChannelUnavailable
}
