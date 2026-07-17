package home

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type livenessDownWatcher struct{ h *Home }

type localIdleArbiter struct {
	h  *Home
	id actor.ActorID
}

func (a localIdleArbiter) RequestIdle(_ context.Context) (bool, error) {
	_, verdict := a.h.liveness.ApproveIdle(a.id)
	if verdict != transitionApplied {
		return false, nil
	}
	if err := a.h.channel.Cells().ApproveIdle(a.id); err != nil {
		// The ledger has already made the actor dormant. A failed command is a
		// resource-tail fault; the next request creates dirty wake debt.
		a.h.logger.Warn("platform.idle.command_failed", "actor", a.id, "err", err)
		return false, err
	}
	a.h.pokeReconcile()
	return false, nil
}

func (h *Home) approveRemoteIdle(_ context.Context, inc actorrt.Incarnation) (bool, error) {
	if !h.channel.Cells().IsLive(inc) {
		return false, actorrt.ErrNotHosted
	}
	_, verdict := h.liveness.ApproveIdle(inc.ID())
	if verdict != transitionApplied {
		return false, nil
	}
	h.pokeReconcile()
	return true, nil
}

// OnDown is the liveness value-edge watcher. It only mutates the in-memory
// ledger and posts a coalesced reconcile wake, so it preserves actorrt's
// non-blocking watcher contract.
func (w livenessDownWatcher) OnDown(_ context.Context, id actor.ActorID, inc actorrt.Incarnation, cause error) {
	h := w.h
	if h.liveness == nil || h.closed.Load() {
		return
	}
	port := h.isIndexedPort(id, inc)
	voluntary := cause == nil || errors.Is(cause, actorbase.ErrIdleExit)
	if h.liveness.ObserveDown(id, port, voluntary) == transitionApplied {
		if !port && !voluntary {
			h.recordBuildFailure(id, time.UnixMilli(h.nowMs()))
		}
		h.pokeReconcile()
	}
}

var _ actorrt.DownWatcher = livenessDownWatcher{}
