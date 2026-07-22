package home_test

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type emptyIntroductionResolver struct{}

func (emptyIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
}
func (emptyIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}
func (emptyIntroductionResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

type emptyCompositionResolver struct{}

func (emptyCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func completeHomeTestConfig(cfg home.Config) home.Config {
	if cfg.CompositionResolver == nil {
		cfg.CompositionResolver = emptyCompositionResolver{}
	}
	if cfg.IntroductionResolver == nil {
		cfg.IntroductionResolver = emptyIntroductionResolver{}
	}
	return cfg
}
