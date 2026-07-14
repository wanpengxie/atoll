package home

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type actorLifecycleGate struct {
	mu sync.Mutex
	m  map[actor.ActorID]*actorLifecycleEntry
}

type actorLifecycleEntry struct {
	mu   sync.Mutex
	refs int
}

func (g *actorLifecycleGate) lock(id actor.ActorID) func() {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[actor.ActorID]*actorLifecycleEntry)
	}
	e := g.m[id]
	if e == nil {
		e = &actorLifecycleEntry{}
		g.m[id] = e
	}
	e.refs++
	g.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		g.mu.Lock()
		e.refs--
		if e.refs == 0 && g.m[id] == e {
			delete(g.m, id)
		}
		g.mu.Unlock()
	}
}
