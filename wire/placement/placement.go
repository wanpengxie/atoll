// Package placement is the actor→compute assignment registry. It tracks which
// business actor is hosted by which attached compute. Zero fencing —排他由
// substrate 结构保证（actorrt connect-in REPLACE + channel 单写路径）。
// Pure schema + in-memory registry, kernel-only dependency.
package placement

import (
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

type Registry struct {
	mu        sync.RWMutex
	byActor   map[actor.ActorID]string
	byCompute map[string]map[actor.ActorID]struct{}
}

func New() *Registry {
	return &Registry{
		byActor:   make(map[actor.ActorID]string),
		byCompute: make(map[string]map[actor.ActorID]struct{}),
	}
}

func (r *Registry) Assign(actorID actor.ActorID, computeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.byActor[actorID]; ok && old != computeID {
		delete(r.byCompute[old], actorID)
	}
	r.byActor[actorID] = computeID
	if r.byCompute[computeID] == nil {
		r.byCompute[computeID] = make(map[actor.ActorID]struct{})
	}
	r.byCompute[computeID][actorID] = struct{}{}
}

func (r *Registry) Remove(actorID actor.ActorID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cid, ok := r.byActor[actorID]; ok {
		delete(r.byCompute[cid], actorID)
		delete(r.byActor, actorID)
	}
}

func (r *Registry) Lookup(actorID actor.ActorID) (computeID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	computeID, ok = r.byActor[actorID]
	return
}

func (r *Registry) ByCompute(computeID string) []actor.ActorID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := r.byCompute[computeID]
	out := make([]actor.ActorID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func (r *Registry) RemoveCompute(computeID string) []actor.ActorID {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.byCompute[computeID]
	affected := make([]actor.ActorID, 0, len(set))
	for id := range set {
		affected = append(affected, id)
		delete(r.byActor, id)
	}
	delete(r.byCompute, computeID)
	return affected
}
