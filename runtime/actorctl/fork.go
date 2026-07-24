package actorctl

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func freshChildID(parent actor.ActorID, hint string) actor.ActorID {
	if hint == "" {
		hint = "child"
	}
	return actor.ActorID(fmt.Sprintf("%s/%s-%s", parent, hint, uuid.NewString()))
}

func normalizeFork(
	spec actorcaps.ForkSpec,
	parent ActorDefinition,
) (actorcaps.ForkSpec, storespec.Placement, error) {
	if _, ok := actor.ParseKind(string(spec.Kind)); !ok ||
		spec.Kind == actor.KindSystem ||
		spec.Class == "" {
		return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
	}
	if spec.NameHint == "" {
		spec.NameHint = "child"
	}
	if len(spec.NameHint) > 64 {
		return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
	}
	placement := parent.Placement
	if spec.Placement != nil {
		switch spec.Placement.Kind {
		case "server":
			placement = storespec.NewServerPlacement()
		case "daemon":
			var err error
			placement, err = storespec.NewDaemonPlacement(spec.Placement.DesiredHost)
			if err != nil {
				return actorcaps.ForkSpec{}, storespec.Placement{}, err
			}
		default:
			return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
		}
	}
	return spec, placement, nil
}
