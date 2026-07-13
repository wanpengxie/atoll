package platform

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// reviveLogThrottle bounds the attached-host revive-skip log (see
// Home.reviveLogAt).
const reviveLogThrottle = 30 * time.Second

// reconcileSweep runs the home's level-reconcile quartet in the fixed
// activation → closure → expiry → presence order; each step re-checks ctx so a
// cancel between steps stops the sweep before the next component.
//
// Closure (channel.Reconcile) authors a terminal ONLY on the monotone
// closed-forever fact (deregistered / never a member), never on liveness — so a
// receiver whose desired cell the ring has not yet (re)minted this sweep is still
// a registered member and is left untouched (its open requests wait for the
// deadline reaper), regardless of sweep order. Keying closure on the irreversible
// dereg fact instead of a reversible liveness dip dissolves the old
// "not-yet-minted cell mis-scanned as a corpse" hazard; activation is kept first
// as the natural order — re-mint the always-on desired set before the backstops.
func (h *Home) reconcileSweep(ctx context.Context) {
	h.reconcileActivation(ctx)
	if ctx.Err() != nil {
		return
	}
	h.channel.Reconcile(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepExpired(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepPresence(ctx)
}

// sweepPresence enforces fold rows ⊆ (live embodiments ∪ active membership).
// A failed registry read skips the whole pass: treating failure as an empty set
// would erase every member's last testimony.
func (h *Home) sweepPresence(ctx context.Context) {
	rows, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		h.logger.Warn("platform.presence.sweep_registry_failed", "error", err)
		return
	}
	keep := make(map[actor.ActorID]struct{}, len(rows))
	for _, row := range rows {
		keep[row.ID] = struct{}{}
	}
	for _, id := range h.channel.Cells().LiveIDs() {
		keep[id] = struct{}{}
	}
	removed := h.presenceFold.Sweep(func(id actor.ActorID) bool {
		_, ok := keep[id]
		return ok
	})
	if removed > 0 {
		h.presenceSwept.Add(int64(removed))
		h.logger.Debug("platform.presence.swept", "rows", removed)
	}
}

// PresenceSweptCount reports how many testimony rows the reconciliation
// backstop has cleared over this Home's lifetime.
func (h *Home) PresenceSweptCount() int64 {
	return h.presenceSwept.Load()
}

// logReviveAttached logs, throttled per author (reviveLogThrottle), that an
// identity-timer wake was skipped because its author is currently placed on a
// daemon (§10.13 推导2/3) rather than home.
func (h *Home) logReviveAttached(id actor.ActorID, host string) {
	now := time.Now()
	h.reviveLogMu.Lock()
	if last, ok := h.reviveLogAt[id]; ok && now.Sub(last) < reviveLogThrottle {
		h.reviveLogMu.Unlock()
		return
	}
	h.reviveLogAt[id] = now
	h.reviveLogMu.Unlock()
	h.logger.Warn("platform.revive.attached", "channel", string(h.channelID), "actor", string(id), "host", host)
}

// reconcileActivation is the eager-activation half of the reconcile ring: it
// mints the desired−actual difference and deactivates the members it previously
// managed that are no longer desired. It is a substrate assembly-layer mechanism
// (the reconcile ring骨架 lives here in platform, not the actorrt kernel), driven
// by the same ticker as the closure backstop.
//
// desired = 两源之并 (union of two intent sources), read to completion BEFORE any
// mint/despawn: 组合域 — the app-injected DesiredSource (channel_actors intent
// rows); user域 — platform-internal, derived from THIS channel's own registry (the
// per-channel human members, Kind==human && Host==""). The user域 authority lives
// only inside the channel truth born within Open, so the app cannot enumerate it
// (chicken-egg) — the ring derives it itself. Union 原子性: either source read
// failing aborts the WHOLE tick — no arm runs and prevEagerDesired is NOT updated,
// so 削臂 can never diff against a truncated current and evict live cells wholesale.
//
// 补 (revive): for every desired member absent from the live set, resolve its
// factory through factoryFor (human → the platform's own built-in cell factory;
// others → the app-injected组合域 builder), weld caps at the platform seam, and
// SpawnIfAbsent it (the CAS discards the shell if some other path won the race —
// admission or a concurrent Reviver fire). Kind comes from the MEMBERSHIP record
// (rec.Kind, the authority), never re-answered by the builder.
//
// 交集红线: desired = intent ∩ durable membership. A组合域 intent row whose Admit
// never landed (crash between the two non-atomic writes) is not in the membership
// snapshot, so it is skipped BEFORE current[id]=true — a非成员 must never enter the
// 削臂 management set.
//
// 削 (deactivate, 反误杀): the diff is prevEagerDesired − currentDesired, NEVER
// actual − desired. LiveIDs() mixes in system / fork-child / daemon-attach
// embodiments this ring must never evict — the protected categories that never
// enter the desired set (human is NO LONGER protected: it is a managed member now
// — its Admit puts it in desired, its removal takes it out and the 削臂 evicts it,
// membership-gone = true death). The 削 set is every id that was desired in the
// prior tick and is no longer desired.
//
// placement filter (§10.13 推导2/7, S6): both arms consult the membership Host
// column BEFORE embodying/evicting — an id currently placed on a daemon (Host != "")
// is this ring's business in neither direction: 补 must not double-embody an
// already-attached identity locally, and 削 must not evict an embodiment that is
// legitimately live elsewhere.
func (h *Home) reconcileActivation(ctx context.Context) {
	var composed []actorrt.DesiredMember
	if h.desired != nil {
		m, err := h.desired.Members(ctx)
		if err != nil {
			h.logger.Error("platform.reconcile.desired_failed", "channel", string(h.channelID), "err", err)
			return
		}
		composed = m
		if ctx.Err() != nil {
			return
		}
	}
	actives, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		h.logger.Error("platform.reconcile.registry_failed", "channel", string(h.channelID), "err", err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	// The active-membership snapshot serves BOTH roles, read once for atomicity: it
	// is the user域 source AND the 交集红线 membership filter (+ the placement Host
	// read the two arms consult).
	member := make(map[actor.ActorID]storespec.Record, len(actives))
	for _, rec := range actives {
		member[rec.ID] = rec
	}
	desiredIDs := make(map[actor.ActorID]bool, len(composed)+len(actives))
	for _, m := range composed {
		desiredIDs[m.ID] = true
	}
	for _, rec := range actives {
		if rec.Kind == actor.KindHuman && rec.Host == "" {
			desiredIDs[rec.ID] = true
		}
	}

	rt := h.channel.Cells()
	now := time.UnixMilli(h.nowMs())
	actual := make(map[actor.ActorID]bool)
	for _, id := range rt.LiveIDs() {
		actual[id] = true
	}
	current := make(map[actor.ActorID]bool)
	for id := range desiredIDs {
		if ctx.Err() != nil {
			return
		}
		rec, ok := member[id]
		if !ok || !rec.IsActive() {
			continue // 交集红线: desired-but-not-a-durable-member — skip BEFORE current
		}
		current[id] = true
		if actual[id] {
			h.clearReviveBackoff(id)
			continue
		}
		if rec.Host != "" {
			continue // attached elsewhere — not this ring's authority to embody (反误杀)
		}
		if _, held := h.backoffGate(id, now); held {
			// Build backoff active (a prior BuildFailure has not elapsed): skip the
			// build this tick — the SAME account EnsureLive maintains, so the ring and
			// the reviver back a failing actor off in lockstep instead of the ring
			// re-hammering it every tick/poke while the reviver waits. Drop id from
			// current exactly as the build-failure arm below does: a member that never
			// embodied is not carried as managed into prevEagerDesired.
			delete(current, id)
			continue
		}
		factory, ok := h.factoryFor(rec)
		if !ok {
			h.logger.Warn("platform.reconcile.no_factory", "channel", string(h.channelID), "actor", string(id))
			continue
		}
		kind := rec.Kind
		mid := id
		inc, built, buildErr := rt.SpawnIfAbsent(mid, kind, func(inc actorrt.Incarnation) actorrt.Actor {
			return build(h.buildCaps(mid, kind, inc), h.hooks(), factory)
		})
		if ctx.Err() != nil {
			if built {
				rt.Despawn(inc)
			}
			return
		}
		if buildErr != nil {
			if errors.Is(buildErr, actorrt.ErrRuntimeSealed) {
				h.logger.Info("platform.reconcile.runtime_sealed", "channel", string(h.channelID), "actor", string(mid))
				return
			}
			h.logger.Error("platform.reconcile.build_failed", "channel", string(h.channelID), "actor", string(mid), "error", buildErr)
			var failure *actorrt.BuildFailure
			if errors.As(buildErr, &failure) {
				h.recordBuildFailure(mid, now)
			}
			delete(current, mid)
			continue
		}
		// Post-build straddle recheck (shared verifyPostBuild, mirror of the reviver
		// arm's S-P20 recheck): a concurrent Home.Remove (dereg) or daemon attach (Host
		// stamp) can land BETWEEN the Lookup that admitted mid into current above and
		// this build. On a real build (CAS winner), re-read under inc — on any non-OK
		// outcome verifyPostBuild undoes it (pointer-guarded Despawn) and we drop mid
		// from current so it is neither counted managed nor carried into
		// prevEagerDesired: resurrecting a dead-write cell past its dereg (into an
		// unhoused cell, death-后写 window) is the笔 this closes. A lost CAS (!built) is
		// a no-op — some other path already owns the embodiment.
		if built {
			if h.reconcileBuildHook != nil {
				h.reconcileBuildHook(mid)
			}
			if _, res, _ := h.verifyPostBuild(ctx, mid, inc); res != recheckOK {
				delete(current, mid)
			}
		}
	}
	for id := range h.prevEagerDesired {
		if ctx.Err() != nil {
			return
		}
		if current[id] {
			continue
		}
		if rec, ok := member[id]; ok && rec.Host != "" {
			continue // attached elsewhere — not gone, not this ring's to evict (反误杀)
		}
		rt.DespawnID(id)
		// The削 arm's own account cleanup, mirroring Remove's teardown (remove.go):
		// an id this ring stops managing must not leave a permanently stale revive-
		// backoff/log-throttle entry behind (e.g. intent withdrawn for a build-
		// failing member — its backoff account would otherwise never be cleared).
		h.clearReviveBackoff(id)
		h.reviveLogMu.Lock()
		delete(h.reviveLogAt, id)
		h.reviveLogMu.Unlock()
	}
	h.prevEagerDesired = current
}

// factoryFor is the single activation-dispatch point shared by the reconcile
// ring's 补臂 and homeReviver.EnsureLive: it maps a durable member record to the
// ActorFactory that embodies it. A human member (Kind==KindHuman) resolves to the
// platform's OWN built-in human cell factory — user域 supply is platform internal
// 政 (the per-channel human member's authority lives only in this channel's
// registry, unreachable by the app), so the app-injected builder is never asked
// for it. Every other kind resolves through the组合域 builder table (nil builder →
// not-found). Kind is caller-held (rec.Kind), never re-answered.
func (h *Home) factoryFor(rec storespec.Record) (ActorFactory, bool) {
	if rec.Kind == actor.KindHuman {
		// 装配链 step② (gateway 期 v0.4.1 勘误: 槽随户籍准入 ensure): the per-identity
		// binding slot is ensured HERE — the platform-authoritative membership→
		// embodiment dispatch (shared by the reconcile 补臂 and homeReviver.EnsureLive),
		// which covers BOTH a fresh Admit-poke AND a restart's durable re-read (a member
		// read from the store with no Admit call this run). This runs BEFORE the cell is
		// built, so the cell/factory step③ is a pure lookup — never a construction-path
		// self-ensure (装配授权走私). Idempotent; nil registry is a defensive no-op.
		h.EnsureSubjectSlot(rec.ID)
		return humanCellFactory(h, rec.ID), true
	}
	if h.builder == nil {
		return ActorFactory{}, false
	}
	return h.builder.Lookup(rec.ID)
}

// recheckResult classifies a post-build straddle recheck (verifyPostBuild).
type recheckResult int

const (
	recheckOK       recheckResult = iota // still an active home-placed member — keep the build
	recheckGone                          // Remove'd / not a durable member — build undone
	recheckAttached                      // daemon-attached mid-build — build undone
	recheckFault                         // registry lookup fault — build undone
)

// verifyPostBuild is the shared straddle-window closure for every eager LOCAL birth
// (the reconcile ring's 补臂 and homeReviver.EnsureLive): after SpawnIfAbsent mints
// inc, re-read the registry NOW and confirm id is still an active, HOME-placed
// member. The window it closes: a concurrent Home.Remove (dereg) or a daemon attach
// (Host stamp) landing BETWEEN the pre-build Lookup and this build's landing. On any
// non-OK outcome it UNDOES the build — Despawn is pointer-guarded (evicts IFF the
// runtime map still points to THIS inc, so a legitimate successor that already
// re-admitted the id is never evicted) — and returns the classification so each
// caller maps it to its own contract (EnsureLive → transient err / poison verdict;
// the ring → drop from current/managed). rec carries the fresh Host for the
// attached case's log; err is set only for recheckFault (the caller wraps it).
func (h *Home) verifyPostBuild(ctx context.Context, id actor.ActorID, inc actorrt.Incarnation) (rec storespec.Record, res recheckResult, err error) {
	rec2, ok2, lerr := h.cs.Registry.Lookup(ctx, id)
	if ctx.Err() != nil {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckFault, ctx.Err()
	}
	if lerr != nil {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckFault, lerr
	}
	if !ok2 || !rec2.IsActive() {
		h.channel.Cells().Despawn(inc)
		// 死 ID 槽级联清 (gateway 期 P1, mirror remove.go §级联删槽): factoryFor embodies a
		// human by EnsureSubjectSlot BEFORE this build (装配链 step②). A stale-rec 补臂/
		// EnsureLive whose Lookup predated a concurrent Home.Remove can therefore RE-create
		// the binding slot AFTER Remove's RemoveSubjectSlot ran — resurrecting a dead id's
		// slot. Despawn alone (above)只 evicts the cell, not the slot, so close the笔 here
		// with the SAME idempotent cascade Remove uses (id is confirmed dead — 身份不可复活
		// mints a fresh id on any re-Admit, so this only ever targets THIS dead id, never a
		// live successor). A no-op for a non-human (no slot was ever ensured) and when
		// Remove already cleaned it.
		h.RemoveSubjectSlot(id)
		h.presenceFold.Forget(id)
		return storespec.Record{}, recheckGone, nil
	}
	return rec2, recheckOK, nil
}

// pokeReconcile posts a coalesced wake to the reconcile ring (non-blocking: a
// full buffer already carries the pending edge). No-op if the ticker goroutine
// has not launched yet (genesis is covered by the synchronous startup sweep).
func (h *Home) pokeReconcile() {
	if h.pokeCh == nil {
		return
	}
	select {
	case h.pokeCh <- struct{}{}:
	default:
	}
}
