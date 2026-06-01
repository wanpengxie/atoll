package runtime

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
)

// This file holds the channel's in-daemon actors as real objects
// (actorrt.Actor), replacing the former stateless handler closures
// registered on scheduler.Deliverer. Each is spawned as a cell on the
// channel's actorrt.Runtime; the cell's single goroutine serialises Receive
// so these types hold their own state without locks.

var (
	_ actorrt.Actor = (*systemActor)(nil)
	_ actorrt.Actor = (*agentActor)(nil)
)

// systemActor is the channel system actor (actor.SystemActorID) as an
// object, replacing systemActorHandler. It holds no mutable state: the
// actor/type catalog is read live from the registry on every actor.list
// (INVARIANT-2, no frozen snapshot). Any request other than actor.list is
// ignored (no error) — closure for an unanswered request is the sender's
// caller-scoped responsibility, not a global fallback.
type systemActor struct {
	daemon *Daemon
	cr     *channelRuntime
}

// Receive implements actorrt.Actor.
func (s *systemActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env == nil || env.Kind != message.KindRequest || env.Type != "actor.list" {
		return nil
	}
	return s.daemon.respondActorList(ctx, s.cr, env)
}

// agentActor is the channel agent target (cr.channelAgentID) as an object,
// replacing the deliverer handler that forwarded triggers to the worker
// bridge. The worker itself is the cross-process actor; this in-daemon cell
// is its façade, forwarding each trigger over IPC. When bridge is nil it is
// the P2 counter-only fallback.
type agentActor struct {
	cr     *channelRuntime
	bridge *workerhost.Bridge
}

// Receive implements actorrt.Actor.
func (a *agentActor) Receive(ctx context.Context, env *message.Envelope) error {
	a.cr.channelAgentTriggers.Add(1)
	if a.bridge == nil {
		return nil
	}
	return a.bridge.OnTrigger(ctx, a.cr.channelAgentID, env)
}

// channelSupervisor implements actorrt.Supervisor: when a cell's actor
// goroutine dies (panic in Receive/Start/Stop), the substrate closes every
// in-flight request addressed to the dead actor with a system-authored
// receiver_unavailable terminal (construction-spec §3.3). This is the
// substrate's ONLY closure obligation — it writes death it positively
// observed, never guesses "slow". Replaces Manager recover→
// receiver_internal_error for whole-actor death. cr is set after the
// channelRuntime is constructed (cells are created before cr).
type channelSupervisor struct {
	daemon *Daemon
	cr     *channelRuntime
}

// OnDeath implements actorrt.Supervisor.
func (s *channelSupervisor) OnDeath(ctx context.Context, sig actorrt.DeathSignal) {
	if s.cr == nil || s.cr.messages == nil || s.cr.wrappedChain == nil {
		return
	}
	reqs, err := s.cr.messages.OpenRequestsForActor(ctx, sig.Actor, 256)
	if err != nil {
		s.daemon.log.Warn().Err(err).
			Str("event", "runtime.death_signal_scan_failed").
			Str("actor_id", string(sig.Actor)).
			Msg("death signal: scan open requests failed")
		return
	}
	for i := range reqs {
		s.daemon.writeSubstrateUnavailable(ctx, s.cr, &reqs[i], sig.Cause)
	}
}
