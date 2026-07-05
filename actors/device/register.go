package device

import (
	"errors"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func init() { registry.Register("device", registry.ClassDecl{Kind: actor.KindTool, New: construct}) }

// construct: the generic device actor. This is a true essence-singleton:
// the instance's identity IS the external resource (the machine), so the id
// is DERIVED from the device identity, not taken from the spec — a second
// instance of the same device is incoherent. ctx.DeviceName is the identity;
// spec.ID is ignored.
func construct(_ registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	if ctx.DeviceName == "" {
		return platform.ActorDecl{}, errors.New("device: empty device name")
	}
	id := actor.ActorID("device:" + ctx.DeviceName)
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Factory: platform.ActorFactory{Legacy: func(pen harness.Pen) actorrt.Actor { return NewActor(pen, id, ctx.WorkspaceDir, ctx.Logger) }},
	}, nil
}
