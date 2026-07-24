package actorctl

import (
	"bytes"
	"sort"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type controlGate struct {
	mu   sync.Mutex
	refs int
}

type controlGates struct {
	mu    sync.Mutex
	gates map[actor.ActorID]*controlGate
}

func (g *controlGates) ref(id actor.ActorID) *controlGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gates == nil {
		g.gates = make(map[actor.ActorID]*controlGate)
	}
	gate := g.gates[id]
	if gate == nil {
		gate = &controlGate{}
		g.gates[id] = gate
	}
	gate.refs++
	return gate
}

func (g *controlGates) unref(id actor.ActorID, gate *controlGate) {
	g.mu.Lock()
	gate.refs--
	if gate.refs == 0 {
		delete(g.gates, id)
	}
	g.mu.Unlock()
}

func (g *controlGates) lock(id actor.ActorID) func() {
	gate := g.ref(id)
	gate.mu.Lock()
	return func() {
		gate.mu.Unlock()
		g.unref(id, gate)
	}
}

func canonicalActorIDs(ids []actor.ActorID) []actor.ActorID {
	set := make(map[actor.ActorID]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			set[id] = struct{}{}
		}
	}
	out := make([]actor.ActorID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare([]byte(out[i]), []byte(out[j])) < 0
	})
	return out
}

func (g *controlGates) lockActorSet(ids []actor.ActorID) func() {
	ordered := canonicalActorIDs(ids)
	releases := make([]func(), 0, len(ordered))
	for _, id := range ordered {
		releases = append(releases, g.lock(id))
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
}
