package home_test

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type emptyCompositionResolver struct{}

func (emptyCompositionResolver) ResolveComposition(context.Context, channel.ID, storespec.CompositionRecord) (platform.ActorDecl, bool, error) {
	return platform.ActorDecl{}, false, nil
}

func (emptyCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

type allowTestDaemonAuthority struct{}

func (allowTestDaemonAuthority) LockAndValidate(context.Context, string, channel.ID) (func(), error) {
	return func() {}, nil
}

func completeHomeTestConfig(cfg home.Config) home.Config {
	if cfg.CompositionResolver == nil {
		cfg.CompositionResolver = emptyCompositionResolver{}
	}
	if cfg.DaemonAuthority == nil {
		cfg.DaemonAuthority = allowTestDaemonAuthority{}
	}
	return cfg
}
