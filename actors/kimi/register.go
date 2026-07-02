package kimi

import (
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func init() { registry.Register("kimi", construct) }

// construct: browser-extension adapter (mirrors xhs; differs only by id+addr).
// id comes from the spec; blank → class default. The listen addr is config (a
// transport detail, NOT an essence-singleton constraint — two kimi instances on
// different addrs are legal); day-0 uses the default addr.
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	id := spec.ID
	if id == "" {
		id = DefaultActorID
	}
	cfg := Config{ListenAddr: DefaultListenAddr, Logger: ctx.Logger}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
		Factory: func(caps actorcaps.Caps) actorrt.Actor { return NewActor(caps.Pen, cfg) },
	}, nil
}
