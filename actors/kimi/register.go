package kimi

import (
	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("kimi", decl) }

// decl: browser-extension adapter (mirrors xhs; differs only by id+addr).
func decl(d registry.Deps) (platform.ActorDecl, bool, error) {
	cfg := Config{ListenAddr: DefaultListenAddr, Logger: d.Logger}
	return platform.ActorDecl{
		ID:      DefaultActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
		Factory: func(w harness.Writer) actorrt.Actor { return NewActor(w, cfg) },
	}, true, nil
}
