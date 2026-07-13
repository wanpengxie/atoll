package home

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// reviveLogThrottle bounds the attached-host revive-skip log (see
// Home.reviveLogAt).
const reviveLogThrottle = 30 * time.Second

// noFactoryWarned edge-dedups the platform.reconcile.no_factory Warn (B3): an
// actor ID is minted once per lifetime (never reused — see identity-instance-
// spec), so a package-level set keyed by ActorID is safe across every Home
// without threading a new field through the Home struct (out of this file's
// change scope). Guarded by noFactoryWarnedMu since multiple Homes' reconcile
// ticks run concurrently. Entry is set on first no_factory observation for an
// id and cleared the moment that id resolves to any other activation verdict
// or stops being managed by this ring — so steady-state no_factory (soak's
// 74x repeat) goes silent after the first Warn, without ever going fully mute
// across restarts (fresh process = fresh empty set = first sighting warns).
var (
	noFactoryWarnedMu sync.Mutex
	noFactoryWarned   = make(map[actor.ActorID]struct{})
)

// clearNoFactoryWarned drops id's no_factory edge marker, e.g. once a factory
// becomes available or the id is no longer this ring's to manage.
func clearNoFactoryWarned(id actor.ActorID) {
	noFactoryWarnedMu.Lock()
	delete(noFactoryWarned, id)
	noFactoryWarnedMu.Unlock()
}

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
	h.reviveMu.Lock()
	if last, ok := h.reviveLogAt[id]; ok && now.Sub(last) < reviveLogThrottle {
		h.reviveMu.Unlock()
		return
	}
	h.reviveLogAt[id] = now
	h.reviveMu.Unlock()
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
// activationOutcome is the 11-word closed set every eager-activation attempt
// resolves to (spec §1.3, purity v3 B1) — the single vocabulary activateOne
// speaks; reconcileActivation's 环翻译器 (§1.6) and homeReviver.EnsureLive's
// reviver 翻译器 (§1.7) each map it onto their own control-flow / log / account
// contract. No recheckAttached-shaped word exists (§0.5-A: the placement
// straddle race self-resolves via Attach's own replace semantics, never via a
// post-build recheck — see verifyPostBuild's doc).
type activationOutcome int

const (
	actEmbodied     activationOutcome = iota // built and confirmed live: inc payload
	actAlreadyLive                           // ⑤ CAS loser: some other path already owns the embodiment
	actNotMember                             // ①防御复判 tripped (defensive-only; both callers pre-filter this fact)
	actAttached                              // ②Host 判: placed on a daemon, not this seam's authority: host payload
	actBackoffHeld                           // ③backoffGate: prior BuildFailure has not elapsed: until payload
	actNoFactory                             // ④factoryFor miss: reason payload ("no_builder" | "class_not_found")
	actSealed                                // buildErr is ErrRuntimeSealed: err payload
	actBuildFailed                           // buildErr is a deterministic *actorrt.BuildFailure: err payload
	actCancelled                             // ctx cancelled right after SpawnIfAbsent (built cell already despawned)
	actRecheckFault                          // ⑥verifyPostBuild's own ctx/registry fault: err payload
	actRecheckGone                           // ⑥verifyPostBuild found the id no longer an active member
)

// activationVerdict pairs an activationOutcome word with its payload (§1.3:
// "unexported 常量族 + 载荷"). Only the field(s) named in the outcome's own
// comment above are meaningful for a given kind.
type activationVerdict struct {
	kind   activationOutcome
	inc    actorrt.Incarnation
	host   string
	until  time.Time
	reason string
	err    error
}

// activateOne is the single eager-activation core shared by reconcileActivation
// (环补臂) and homeReviver.EnsureLive (spec §1.1): given a durable member record
// the CALLER has already resolved (event-read stays outside the core — §1.8,
// the batch ListActive snapshot vs the single-point Lookup and its transient
// semantics belong to each caller's own translator), it runs the six-gate
// activation sequence in the FIXED order below (§1.2, P1-C — do not reorder)
// and returns one word from the 11-word closed set. It writes nothing to any
// account except recordBuildFailure (the ONE ledger write inside the core —
// the two former call sites were byte-for-byte identical, §1.1); every other
// side effect (log, backoff clear, current-map bookkeeping) is the caller's
// translator's job, never the core's.
//
// Both hooks now fire on BOTH callers' paths (straddle in the ring too, build
// in the reviver too — §1.5, P2-C): a future test that sets one hook while
// driving the OTHER caller will observe it fire where it previously would not
// have. Current test fixtures are safe (ReconcileInterval=1h, no cross-driving
// wake), but this is the risk surface to check first if a new hook-driven test
// misbehaves.
func (h *Home) activateOne(ctx context.Context, rec storespec.Record) activationVerdict {
	// ①IsActive 防御复判: for the ring this is pure defense-in-depth (环选择器
	// already filters !rec.IsActive() before setting current[id] true and
	// calling here — rec is a value copy, so this branch can never observe a
	// DIFFERENT fact than the selector just saw). For the reviver this is NOT
	// merely defensive — EnsureLive only screens the "!ok, no row at all" case
	// itself (never calling rec.IsActive() on a zero-value Record, whose zero
	// DeregisteredAt would misread as active) and hands every EXISTING row
	// straight here, so a genuinely deregistered-between-Lookup-and-here member
	// is caught by THIS check (DoD §4.2's 3-site IsActive() closure: 环选择器 /
	// here / verifyPostBuild — EnsureLive itself makes no separate call).
	if !rec.IsActive() {
		return activationVerdict{kind: actNotMember}
	}
	// ②Host 判 (placement filter, §10.13 推导2/7, S6): an id currently placed on
	// a daemon is this seam's business in neither direction — 补 must not
	// double-embody an already-attached identity locally.
	if rec.Host != "" {
		return activationVerdict{kind: actAttached, host: rec.Host}
	}
	id := rec.ID
	kind := rec.Kind
	now := time.UnixMilli(h.nowMs())
	// ③backoffGate: a prior BuildFailure not yet elapsed skips the build this
	// tick/wake — the SAME account both callers maintain in lockstep.
	if until, held := h.backoffGate(id, now); held {
		return activationVerdict{kind: actBackoffHeld, until: until}
	}
	// ④factoryFor: the single activation-dispatch point (human → platform's
	// own built-in factory; other kinds → the app-injected builder table).
	factory, ok := h.factoryFor(rec)
	if !ok {
		reason := "class_not_found"
		if h.builder == nil {
			reason = "no_builder"
		}
		return activationVerdict{kind: actNoFactory, reason: reason}
	}
	// reviverStraddleHook (test-only, nil in production): the S-P20 straddle
	// seam — fires AFTER factoryFor resolves, BEFORE the CAS below.
	if h.reviverStraddleHook != nil {
		h.reviverStraddleHook()
	}
	// ⑤SpawnIfAbsent: the atomic placement CAS — discards the shell if some
	// other path (admission, a concurrent revive) already won the race.
	inc, built, buildErr := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, kind, inc), h.hooks(), factory)
	})
	// ctx 闸 (P1-C): checked BEFORE buildErr classification and BEFORE the
	// !built branch — a cancel landing here wins over either. Distinct from
	// verifyPostBuild's OWN internal ctx→recheckFault gate below (P1-B): this
	// one closes the window right after the CAS; that one closes the window
	// around the post-build registry re-read.
	if ctx.Err() != nil {
		if built {
			h.channel.Cells().Despawn(inc)
		}
		return activationVerdict{kind: actCancelled}
	}
	if buildErr != nil {
		if errors.Is(buildErr, actorrt.ErrRuntimeSealed) {
			return activationVerdict{kind: actSealed, err: buildErr}
		}
		var failure *actorrt.BuildFailure
		if errors.As(buildErr, &failure) {
			h.recordBuildFailure(id, now)
		}
		return activationVerdict{kind: actBuildFailed, err: buildErr}
	}
	if !built {
		return activationVerdict{kind: actAlreadyLive}
	}
	// reconcileBuildHook (test-only, nil in production): fires on a REAL CAS
	// win, right before the post-build recheck below.
	if h.reconcileBuildHook != nil {
		h.reconcileBuildHook(id)
	}
	// ⑥verifyPostBuild: the shared straddle-window closure (its own internal
	// ctx gate is left untouched inside it, P1-B).
	_, res, verr := h.verifyPostBuild(ctx, id, inc)
	switch res {
	case recheckGone:
		return activationVerdict{kind: actRecheckGone}
	case recheckFault:
		return activationVerdict{kind: actRecheckFault, err: verr}
	default: // recheckOK
		return activationVerdict{kind: actEmbodied, inc: inc}
	}
}

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
		// 补: resolve activateOne's 11-word verdict (§1.6 环翻译器) — the core
		// covers everything from here through the post-build straddle recheck
		// (a concurrent Home.Remove landing BETWEEN the Lookup that admitted id
		// into current above and the build is the window verifyPostBuild
		// closes; the placement-attach race is a DIFFERENT window, closed by
		// Attach's own replace semantics rather than a post-build recheck —
		// #24, see verifyPostBuild's doc).
		switch v := h.activateOne(ctx, rec); v.kind {
		case actEmbodied, actAlreadyLive, actAttached:
			// current stays true (already set above); no log; no account write
			// (环翻译器 never clears the backoff account — §1.4 现状, CAS-loser
			// included: reserved to the fast-path arm above and to 削 below).
			clearNoFactoryWarned(id) // left no_factory (a factory now resolves) — B3 edge reset
		case actNotMember:
			// Defensive-only (①防御复判): unreachable given the selector's own
			// pre-filter two lines up, on the SAME rec — treated conservatively
			// like BackoffHeld/BuildFailed rather than left counted as managed.
			delete(current, id)
		case actBackoffHeld:
			delete(current, id)
		case actNoFactory:
			// B3: edge-only — warn once per id on first no_factory sighting,
			// silent through steady-state repeats (soak observed 74x/pass without
			// this dedup). clearNoFactoryWarned resets the marker once the id
			// leaves this state (factory resolves, or the ring stops managing it).
			noFactoryWarnedMu.Lock()
			_, alreadyWarned := noFactoryWarned[id]
			if !alreadyWarned {
				noFactoryWarned[id] = struct{}{}
			}
			noFactoryWarnedMu.Unlock()
			if !alreadyWarned {
				h.logger.Warn("platform.reconcile.no_factory", "channel", string(h.channelID), "actor", string(id))
			}
		case actSealed:
			h.logger.Info("platform.reconcile.runtime_sealed", "channel", string(h.channelID), "actor", string(id))
			return // whole tick aborts — prevEagerDesired stays exactly the pre-tick baseline
		case actBuildFailed:
			h.logger.Error("platform.reconcile.build_failed", "channel", string(h.channelID), "actor", string(id), "error", v.err)
			delete(current, id)
		case actCancelled:
			return // whole tick aborts; the built cell (if any) was despawned inside the core
		case actRecheckFault, actRecheckGone:
			delete(current, id)
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
		h.reviveMu.Lock()
		delete(h.reviveLogAt, id)
		h.reviveMu.Unlock()
		clearNoFactoryWarned(id) // ring stopped managing id — B3 edge reset
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
func (h *Home) factoryFor(rec storespec.Record) (platform.ActorFactory, bool) {
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
		return platform.ActorFactory{}, false
	}
	return h.builder.Lookup(rec.ID)
}

// recheckResult classifies a post-build straddle recheck (verifyPostBuild).
type recheckResult int

const (
	recheckOK    recheckResult = iota // still an active member — keep the build
	recheckGone                       // Remove'd / not a durable member — build undone
	recheckFault                      // registry lookup fault — build undone
)

// verifyPostBuild is the shared straddle-window closure for every eager LOCAL birth
// (the reconcile ring's 补臂 and homeReviver.EnsureLive, via activateOne): after
// SpawnIfAbsent mints inc, re-read the registry NOW and confirm id is still an
// active member. The window it closes: a concurrent Home.Remove (dereg) landing
// BETWEEN the pre-build Lookup and this build's landing. On a non-OK outcome it
// UNDOES the build — Despawn is pointer-guarded (evicts IFF the runtime map still
// points to THIS inc, so a legitimate successor that already re-admitted the id is
// never evicted) — and returns the classification so activateOne maps it onto its
// own 11-word verdict. err is set only for recheckFault (the caller wraps it).
//
// #24 (§0.5-A, owner-拍定): this recheck is ACTIVE-ONLY — it does NOT re-compare
// Host. A concurrent daemon attach landing in this same window is a DIFFERENT race
// than dereg, and is closed by a DIFFERENT mechanism: Attach's own replace
// semantics (whichever of {this build, the attach} lands second wins outright —
// build-then-attach: attach's CAS/replace supersedes the local cell; attach-then-
// build: this build's own SpawnIfAbsent CAS loses outright, §1.3 actAlreadyLive),
// so no interleaving ever leaves two live embodiments, and a post-build Host
// re-check here would only OPEN a worse window (undoing a build that already lost
// fairly, leaving neither side embodied). IDM's id-never-reused invariant (S-8,
// w3-glue-batch-requirements.md) is what let this recheck shrink to a single
// active query in the first place — a registration can never flip back and forth
// under the same id, so "was active, now inactive" is the only fact worth
// re-reading. See TestReviver_AttachRace_ReplaceSemanticsSelfResolve (formerly
// AttachStraddle) for the pinned behavior this account describes.
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
