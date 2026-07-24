package actorctl

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type forkAdmission struct {
	child     actor.ActorID
	found     bool
	spec      actorcaps.ForkSpec
	placement storespec.Placement
}

// admitFork confines the caller gate to the one logical admission verdict.
// Accepted work leaves this function without any caller gate held; Store commit
// and child publication are trusted continuations under their own owners.
func (c *Controller) admitFork(
	ctx context.Context,
	request ForkRequest,
) (forkAdmission, error) {
	unlock := c.gates.lock(request.CallerActorID)
	defer unlock()

	if child, found, err := c.store.LookupFork(
		ctx,
		request.CallerActorID,
		request.RequestID,
	); err != nil {
		return forkAdmission{}, err
	} else if found {
		return forkAdmission{child: child, found: true}, nil
	}
	if err := c.checkCurrentSnapshot(
		request.CallerActorID,
		request.CallerAttempt,
	); err != nil {
		return forkAdmission{}, err
	}
	parent, ok, err := c.Lookup(request.CallerActorID)
	if err != nil {
		return forkAdmission{}, err
	}
	if !ok {
		return forkAdmission{}, ErrInactive
	}
	spec, placement, err := normalizeFork(request.Spec, parent.Definition)
	if err != nil {
		return forkAdmission{}, err
	}
	return forkAdmission{spec: spec, placement: placement}, nil
}

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
