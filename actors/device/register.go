package device

import (
	"github.com/wanpengxie/ActOS/actors/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("device", decl) }

// decl: the generic device actor — attaching a daemon means attaching a device,
// so it applies whenever a daemon runs (id carries the device identity).
func decl(d registry.Deps) (platform.ActorDecl, bool, error) {
	id := actor.ActorID("device:" + d.DeviceName)
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(w harness.Writer) actorrt.Actor { return NewActor(w, id, d.WorkspaceDir, d.Logger) },
	}, true, nil
}
