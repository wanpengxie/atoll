package echo

import (
	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
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
		Factory: func(w harness.Writer) actorrt.Actor { return NewActor(w) },
	}, nil
}
