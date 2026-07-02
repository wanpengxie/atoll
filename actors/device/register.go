package device

import (
	"errors"

	"github.com/wanpengxie/ActOS/lib/actorcaps"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

func init() { registry.Register("device", construct) }

// construct: the generic device actor. This is the one TRUE essence-singleton
// (actor-instance-model §5.1): the instance's identity IS the external resource
// (the machine), so the id is DERIVED from the device identity, not taken from
// the spec — a second instance of the same device is incoherent. ctx.DeviceName
// is the identity; spec.ID is ignored.
func construct(_ registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	if ctx.DeviceName == "" {
		return platform.ActorDecl{}, errors.New("device: empty device name")
	}
	id := actor.ActorID("device:" + ctx.DeviceName)
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
		Factory: func(caps actorcaps.Caps) actorrt.Actor { return NewActor(caps.Pen, id, ctx.WorkspaceDir, ctx.Logger) },
	}, nil
}
