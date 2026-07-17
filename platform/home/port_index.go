package home

import (
	"context"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type homePortEntry struct {
	owner link.PortOwner
	inc   actorrt.Incarnation
}

type homePortIndex struct{ h *Home }

func (x homePortIndex) Register(owner link.PortOwner, inc actorrt.Incarnation, ticket string, birthVersion int64) bool {
	if x.h.liveness == nil || x.h.liveness.Attach(inc.ID(), EnsureTicket(ticket), birthVersion, inc, runtimeDeliveryCarrier{id: inc.ID(), deliverer: x.h.channel.Deliverer()}) != transitionApplied {
		return false
	}
	x.h.indexMu.Lock()
	x.h.portIndex[inc.ID()] = homePortEntry{owner: owner, inc: inc}
	x.h.indexMu.Unlock()
	x.h.redeliverOpenRequests(context.Background(), inc.ID())
	return true
}

func (x homePortIndex) Remove(owner link.PortOwner, inc actorrt.Incarnation) {
	x.h.indexMu.Lock()
	if cur, ok := x.h.portIndex[inc.ID()]; ok && cur.owner == owner && cur.inc == inc {
		delete(x.h.portIndex, inc.ID())
		if x.h.liveness != nil {
			_ = x.h.liveness.ObserveDown(inc.ID(), inc, true, false)
		}
	}
	x.h.indexMu.Unlock()
}

func (x homePortIndex) Take(owner link.PortOwner, id actor.ActorID) (actorrt.Incarnation, bool) {
	x.h.indexMu.Lock()
	defer x.h.indexMu.Unlock()
	cur, ok := x.h.portIndex[id]
	if !ok || cur.owner != owner {
		return actorrt.Incarnation{}, false
	}
	delete(x.h.portIndex, id)
	return cur.inc, true
}

func (x homePortIndex) TakeOwner(owner link.PortOwner) []actorrt.Incarnation {
	x.h.indexMu.Lock()
	defer x.h.indexMu.Unlock()
	var out []actorrt.Incarnation
	for id, cur := range x.h.portIndex {
		if cur.owner == owner {
			out = append(out, cur.inc)
			delete(x.h.portIndex, id)
		}
	}
	return out
}

func (x homePortIndex) ExpireOwner(owner link.PortOwner) {
	x.h.indexMu.Lock()
	ids := make([]actor.ActorID, 0)
	for id, cur := range x.h.portIndex {
		if cur.owner == owner {
			ids = append(ids, id)
		}
	}
	x.h.indexMu.Unlock()
	changed := false
	for _, id := range ids {
		if _, verdict := x.h.liveness.Retire(id, true); verdict == transitionApplied {
			changed = true
		}
	}
	if changed {
		x.h.pokeReconcile()
	}
}

func (h *Home) indexedPortIDs(owner link.PortOwner) []actor.ActorID {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	var ids []actor.ActorID
	for id, cur := range h.portIndex {
		if cur.owner == owner {
			ids = append(ids, id)
		}
	}
	return ids
}

func (h *Home) takeAnyIndexedPort(id actor.ActorID) (actorrt.Incarnation, bool) {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	cur, ok := h.portIndex[id]
	if ok {
		delete(h.portIndex, id)
	}
	return cur.inc, ok
}

func (h *Home) isIndexedPort(id actor.ActorID, inc actorrt.Incarnation) bool {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	cur, ok := h.portIndex[id]
	return ok && cur.inc == inc
}
