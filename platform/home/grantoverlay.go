package home

import (
	"context"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type actorGrantOverlay struct {
	mu         sync.RWMutex
	byResource map[resource.ResourceID]map[actor.ActorID]map[access.Operation]struct{}
}

func newActorGrantOverlay() *actorGrantOverlay {
	return &actorGrantOverlay{byResource: make(map[resource.ResourceID]map[actor.ActorID]map[access.Operation]struct{})}
}

func (o *actorGrantOverlay) ActorAllows(_ context.Context, id actor.ActorID, rid resource.ResourceID, op access.Operation) (bool, error) {
	o.mu.RLock()
	_, ok := o.byResource[rid][id][op]
	o.mu.RUnlock()
	return ok, nil
}

func (o *actorGrantOverlay) SetGrant(_ context.Context, rid resource.ResourceID, grant access.Grant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	actors := o.byResource[rid]
	if actors == nil {
		actors = make(map[actor.ActorID]map[access.Operation]struct{})
		o.byResource[rid] = actors
	}
	if len(grant.Ops) == 0 {
		delete(actors, grant.Grantee)
		if len(actors) == 0 {
			delete(o.byResource, rid)
		}
		return nil
	}
	ops := make(map[access.Operation]struct{}, len(grant.Ops))
	for _, op := range grant.Ops {
		ops[op] = struct{}{}
	}
	actors[grant.Grantee] = ops
	return nil
}

func (o *actorGrantOverlay) EndBatch(ids []actor.ActorID) {
	o.mu.Lock()
	for rid, actors := range o.byResource {
		for _, id := range ids {
			delete(actors, id)
		}
		if len(actors) == 0 {
			delete(o.byResource, rid)
		}
	}
	o.mu.Unlock()
}
