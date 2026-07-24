package home

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// homeActorEffects is the composition-root tail of committed Controller
// transitions. It never mutates Controller truth and never owns execution.
type homeActorEffects struct{ home *Home }

func (e homeActorEffects) PlanPoke(domain actorhost.ExecutionDomain) {
	if e.home != nil {
		e.home.pokeReconcile()
		if e.home.links != nil {
			e.home.links.PokePlan(string(domain))
		}
	}
}

func (e homeActorEffects) ApplyPostCommit(effects storespec.PostCommitEffects) {
	h := e.home
	if h == nil {
		return
	}
	if effects.KickDaemon != nil && h.links != nil {
		h.links.KickDaemon(string(*effects.KickDaemon))
	}
	for _, principal := range effects.Principals {
		if principal != "" && h.onMembershipChange != nil {
			h.onMembershipChange(principal)
		}
	}
	if effects.Poke {
		h.pokeReconcile()
	}
}

func (e homeActorEffects) RunActorBorn(id actor.ActorID) error {
	if e.home == nil || e.home.stateHandles == nil {
		return nil
	}
	return e.home.stateHandles.AdmitRun(id)
}

func (e homeActorEffects) RunActorsEnded(ids []actor.ActorID) {
	h := e.home
	if h == nil {
		return
	}
	if h.stateHandles != nil {
		h.stateHandles.EndBatch(ids)
	}
	if h.grantOverlay != nil {
		h.grantOverlay.EndBatch(ids)
	}
	for _, id := range ids {
		if h.presenceFold != nil {
			h.presenceFold.Forget(id)
		}
	}
	h.pokeReconcile()
}

func (e homeActorEffects) Fatal(err error) {
	h := e.home
	if h == nil {
		return
	}
	h.logger.Error("platform.home.system_kernel_failed", "err", err)
	go func() { _ = h.closeInternal("system_kernel_failed") }()
}

// homeHostEvents projects current local Body observations into presence. Host
// already exact-filters successor-stale events before this sink is called.
type homeHostEvents struct{ home *Home }

func (e homeHostEvents) OnBodyExited(
	id actor.ActorID,
	_ actorhost.AttemptKey,
	self actorrt.Incarnation,
	cause error,
) {
	if e.home != nil && e.home.presenceFold != nil {
		e.home.presenceFold.OnDown(nil, id, self, cause)
	}
}

func (e homeHostEvents) OnBodyObs(
	id actor.ActorID,
	_ actorhost.AttemptKey,
	self actorrt.Incarnation,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	if e.home != nil && e.home.presenceFold != nil {
		e.home.presenceFold.OnObs(nil, id, self, kind, value)
	}
}

var _ actorhost.HostEventSink = homeHostEvents{}
