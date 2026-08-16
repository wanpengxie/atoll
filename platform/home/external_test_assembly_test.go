package home_test

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type emptyIntroductionResolver struct{}

func (emptyIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
}
func (emptyIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}
func (emptyIntroductionResolver) ClassPlacement(context.Context, string) (channel.PlacementKind, bool, error) {
	return "", false, nil
}
func (emptyIntroductionResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

type emptyCompositionResolver struct{}

func (emptyCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

type emptyBindingReader struct{}

func (emptyBindingReader) IsBound(context.Context, channel.ID, string) (bool, error) {
	return false, nil
}
func (emptyBindingReader) ListBoundDeviceIDs(context.Context, channel.ID) ([]string, error) {
	return nil, nil
}
func (emptyBindingReader) ChannelDesired(context.Context, channel.ID) (channelspec.ChannelDesiredFacts, bool, error) {
	return channelspec.ChannelDesiredFacts{}, false, nil
}

func completeHomeTestConfig(cfg home.Config) home.Config {
	if cfg.CompositionResolver == nil {
		cfg.CompositionResolver = emptyCompositionResolver{}
	}
	if cfg.IntroductionResolver == nil {
		cfg.IntroductionResolver = emptyIntroductionResolver{}
	}
	if cfg.RegistryBindings == nil {
		cfg.RegistryBindings = emptyBindingReader{}
	}
	return cfg
}
