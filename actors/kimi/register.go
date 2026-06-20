package kimi

import (
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
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
		Factory: func(w harness.Writer) actorrt.Actor { return NewActor(w, cfg) },
	}, nil
}
