package echo

import (
	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("echo", decl) }

// decl: the zero-config tool — only needs a writer.
func decl(registry.Deps) (platform.ActorDecl, bool, error) {
	return platform.ActorDecl{
		ID:      actor.ActorID("echo"),
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor { return NewActor(w) },
	}, true, nil
}
