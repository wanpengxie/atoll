package home

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/platform/lagoon"
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

type emptyBindingReader struct{}

func (emptyBindingReader) IsBound(context.Context, channel.ID, string) (bool, error) {
	return false, nil
}
func (emptyBindingReader) ListBoundDevices(context.Context, channel.ID) ([]lagoon.DeviceRow, error) {
	return nil, nil
}

func completeHomeTestConfig(cfg Config) Config {
	if cfg.CompositionResolver == nil {
		cfg.CompositionResolver = emptyCompositionResolver{}
	}
	if cfg.IntroductionResolver == nil {
		cfg.IntroductionResolver = inertIntroductionResolver{}
	}
	if cfg.RegistryBindings == nil {
		cfg.RegistryBindings = emptyBindingReader{}
	}
	return cfg
}

// introduceHumanForTest and removeActorForTest drive the same frame executor as
// the channel system actor, without exposing a second production mutation face.
func introduceHumanForTest(h *Home, ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if kind != actor.KindHuman {
		return "", &channelspec.OperationError{Code: channelspec.ErrCodeBadPayload, Detail: "human kind required"}
	}
	value, err := h.opEntry.Execute(ctx, "channel.introduce_actor", sysactor.OperateRequest{
		Anchor: uuid.NewString(), Payload: json.RawMessage(`{"kind":"human","principal":"` + principal + `"}`),
	})
	if err != nil {
		return "", err
	}
	return value.(map[string]any)["instance_id"].(actor.ActorID), nil
}

func removeActorForTest(h *Home, ctx context.Context, target actor.ActorID) error {
	payload, _ := json.Marshal(map[string]any{"instance_id": target})
	_, err := h.opEntry.Execute(ctx, "channel.remove_actor", sysactor.OperateRequest{
		Sender: target, Anchor: uuid.NewString(), Payload: payload,
	})
	return err
}

func isChannelUnavailableForTest(err error) bool {
	var opErr *channelspec.OperationError
	if errors.As(err, &opErr) {
		return opErr.Code == channelspec.ErrCodeChannelUnavailable
	}
	var wireErr *sysactor.OperateError
	return errors.As(err, &wireErr) && wireErr.Code == string(channelspec.ErrCodeChannelUnavailable)
}
