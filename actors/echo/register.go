package echo

import (
	"github.com/wanpengxie/ActOS/lib/actorcaps"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

func init() { registry.Register("echo", construct) }

// construct: the zero-config tool. id comes from the spec (multi-capable); a
// blank spec id falls back to the class default "echo" (the one-of-each case).
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	id := spec.ID
	if id == "" {
		id = actor.ActorID("echo")
	}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(caps actorcaps.Caps) actorrt.Actor { return NewActor(caps.Pen) },
	}, nil
}
