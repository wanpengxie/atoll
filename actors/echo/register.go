package echo

import (
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
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
		Factory: platform.ActorFactory{Proc: Def()},
	}, nil
}
