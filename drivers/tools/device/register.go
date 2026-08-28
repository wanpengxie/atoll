package device

import (
	"errors"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("device", registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct})
}

// construct: the generic device actor. The plan names the seat and this
// constructor fills it, exactly like every other class.
//
// It used to derive its own id from ctx.DeviceName instead, on the reasoning
// that a device actor's identity IS the machine. That reasoning was about the
// wrong layer and it made the class unbuildable: the daemon host refuses a
// decl whose id is not the one the plan asked for (drivers/devicehost:
// "class derived a different id"), because a body claiming an unplanned
// identity has no row to be filed under. A seat id is three segments and a
// derived one is two, so the two could never match and this class never once
// started on any daemon.
//
// WHICH machine a seat landed on is the seat's fact, not the tool's: it is the
// member row's desired_host, answered by the registry. So it does not belong in
// the id, and the "one device actor per machine" guarantee moves from something
// the id enforced (while lying to the plan) to something the operator arranges
// by seating it once — a redundant second seat is merely pointless, not
// incoherent.
//
// ctx.DeviceName is still required, and that check is load-bearing: this class
// is PlacementDaemon, and only a daemon host supplies a device name. An empty
// one means the body is being built somewhere it was never meant to run.
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	if ctx.DeviceName == "" {
		return platform.ActorDecl{}, errors.New("device: empty device name")
	}
	return platform.ActorDecl{
		ID:      spec.ID,
		Kind:    actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(ctx.WorkspaceDir, ctx.Logger)},
	}, nil
}
