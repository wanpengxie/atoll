package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type inertIntroductionResolver struct{}

func (inertIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
}
func (inertIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}
func (inertIntroductionResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
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
		return "", &channel.OperationError{Code: channel.ErrCodeBadPayload, Detail: "admit creates human identities"}
	}
	result, err := SystemOps(h).Admit(ctx, channel.AdmitRequest{Ref: "test:admit:" + uuid.NewString(), Principal: principal})
	return result.ActorID, err
}

func removeThroughSysOp(h *Home, ctx context.Context, target actor.ActorID) error {
	_, err := SystemOps(h).Remove(ctx, channel.RemoveRequest{
		Ref: "test:remove:" + uuid.NewString(), Target: target, InitiatorActorID: target,
	})
	return err
}

func systemEndForTest(h *Home) lifecycleEndHandle {
	return lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}}
}

func isChannelUnavailableForTest(err error) bool {
	var opErr *channel.OperationError
	return errors.As(err, &opErr) && opErr.Code == channel.ErrCodeChannelUnavailable
}
