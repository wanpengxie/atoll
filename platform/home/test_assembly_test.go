package home

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
