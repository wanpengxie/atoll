package home

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// reviveLogThrottle bounds the attached-host revive-skip log (see
// Home.reviveLogAt).
const reviveLogThrottle = 30 * time.Second

// clearNoFactoryWarned drops this Home's no_factory edge marker once a factory
// becomes available or the id is no longer this ring's to manage. The map is
// owned by the single reconcile loop, so it needs no cross-Home global lock.
func (h *Home) clearNoFactoryWarned(id actor.ActorID) {
	delete(h.noFactoryWarned, id)
}

func (h *Home) firstNoFactoryWarning(id actor.ActorID) bool {
	if h.noFactoryWarned == nil {
		h.noFactoryWarned = make(map[actor.ActorID]struct{})
	}
	if _, seen := h.noFactoryWarned[id]; seen {
		return false
	}
	h.noFactoryWarned[id] = struct{}{}
	return true
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
	h.reconcileDaemonIntent(ctx)
	if ctx.Err() != nil {
		return
	}
	h.reconcileActivation(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepFired(ctx)
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
	rows, err := h.controlIndex.ListActive(ctx)
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

// presenceSweptCount reports how many testimony rows the reconciliation
// backstop has cleared over this Home's lifetime.
func (h *Home) presenceSweptCount() int64 {
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
// Desired is projected entirely from Home's unified control index. The same
// snapshot includes declared and run-world identities; liveness is the sole
// activation predicate owner, and a read failure aborts the whole sweep.
//
// 补 (revive): for every desired member absent from the live set, resolve its
// factory through factoryFor (human → the platform's own built-in cell factory;
// others → the app-injected组合域 builder), weld caps at the platform seam, and
// SpawnIfAbsent it (the CAS discards the shell if some other path won the race —
// admission or a concurrent Reviver fire). Kind comes from the control row
// (rec.Kind, the authority), never re-answered by the builder.
//
// Admission publishes durable truth before the control row; fork admission
// publishes its event before the run row. Consequently every row considered here
// has already crossed its world's own admission boundary.
//
// 削 (deactivate, 反误杀): the diff is prevEagerDesired − currentDesired, NEVER
// actual − desired. LiveIDs() mixes in system / fork-child / daemon-attach
// embodiments this ring must never evict — the protected categories that never
// enter the desired set (human is NO LONGER protected: it is a managed member now
// — its Admit puts it in desired, its removal takes it out and the 削臂 evicts it,
// membership-gone = true death). The 削 set is every id that was desired in the
// prior tick and is no longer desired.
//
// Placement is declaration intent: server rows are embodied locally, daemon rows
// are projected into daemon plans only when liveness has an ensure attempt.
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
	actRecheckStale                          // ⑥version/placement changed while the shell was built
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
// (环补臂) and homeReviver.EnsureLive (spec §1.1): given an active control row
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
func (h *Home) activateOne(ctx context.Context, control storespec.ActorControlRow) activationVerdict {
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
	id := control.ID
	kind := control.Kind
	current, active, err := h.controlIndex.LookupActive(ctx, id)
	if err != nil || !active || current.CurrentDeclVersion != control.CurrentDeclVersion {
		return activationVerdict{kind: actNotMember, err: err}
	}
	// Placement is declaration truth; actual attachment remains in liveness/ports.
	if control.Placement.Kind == storespec.PlacementDaemon {
		return activationVerdict{kind: actAttached, host: control.Placement.Host}
	}
	if control.Placement.Kind != storespec.PlacementServer {
		return activationVerdict{kind: actNotMember}
	}
	if state, ok := h.liveness.WakeStanding(id); ok && state.Occ == occRunning && state.HasCarrier {
		if _, live := h.channel.Cells().CurrentIncarnation(id); live {
			return activationVerdict{kind: actAlreadyLive}
		}
		// Quiet substrate teardown has no down edge. Reconciliation is the level
		// backstop that repairs the stale carrier value before ensuring again.
		// The repair hands ObserveDown the token of the exact carrier it read —
		// if a successor was published in between, the write self-validation
		// rejects the repair and the next tick re-reads (值范式).
		_ = h.liveness.ObserveDown(id, state.CarrierInc, false, false)
	}
	now := time.UnixMilli(h.nowMs())
	// ③backoffGate: a prior BuildFailure not yet elapsed skips the build this
	// tick/wake — the SAME account both callers maintain in lockstep.
	if until, held := h.backoffGate(id, now); held {
		return activationVerdict{kind: actBackoffHeld, until: until}
	}
	// ④factoryFor: the single activation-dispatch point (human → platform's
	// own built-in factory; other kinds → the required composition resolver).
	factory, ok := h.factoryFor(control)
	if !ok {
		return activationVerdict{kind: actNoFactory, reason: "class_not_found"}
	}
	ticket, ticketVerdict := h.liveness.BeginEnsure(id, control.CurrentDeclVersion)
	if ticketVerdict == transitionInFlight {
		return activationVerdict{kind: actBackoffHeld, until: now}
	}
	if ticketVerdict != transitionApplied {
		if _, active, lookupErr := h.controlIndex.LookupActive(ctx, id); lookupErr == nil && !active {
			return activationVerdict{kind: actNotMember}
		}
		return activationVerdict{kind: actBuildFailed, err: errors.New("platform: liveness ensure rejected")}
	}
	abortEnsure := func() { _ = h.liveness.AbortEnsure(id, ticket) }
	// ⑤SpawnIfAbsent: the atomic placement CAS — discards the shell if some
	// other path (admission, a concurrent revive) already won the race.
	inc, built, buildErr := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, kind, control.CurrentDeclVersion, inc), h.hooks(), factory, actorbase.Options{
			IdleTimeout: control.TIdle,
			IdleArbiter: localIdleArbiter{h: h, id: id},
		})
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
		abortEnsure()
		return activationVerdict{kind: actCancelled}
	}
	if buildErr != nil {
		abortEnsure()
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
		// SpawnIfAbsent returns a zero Incarnation when it lost the race — the
		// incumbent's token comes from the runtime map (if it died in between,
		// skip; the next tick republishes).
		if cur, live := h.channel.Cells().CurrentIncarnation(id); live {
			if h.liveness.PublishLocal(id, ticket, cur, runtimeDeliveryCarrier{id: id, deliverer: h.channel.Deliverer()}) == transitionApplied {
				h.redeliverOpenRequests(ctx, id)
			}
		}
		return activationVerdict{kind: actAlreadyLive}
	}
	// ⑥verifyPostBuild: the shared straddle-window closure (its own internal
	// ctx gate is left untouched inside it, P1-B).
	res, verr := h.verifyPostBuild(ctx, id, control.CurrentDeclVersion, inc)
	switch res {
	case recheckGone:
		abortEnsure()
		return activationVerdict{kind: actRecheckGone}
	case recheckStale:
		abortEnsure()
		return activationVerdict{kind: actRecheckStale}
	case recheckFault:
		abortEnsure()
		return activationVerdict{kind: actRecheckFault, err: verr}
	default: // recheckOK
		if h.liveness.PublishLocal(id, ticket, inc, runtimeDeliveryCarrier{id: id, deliverer: h.channel.Deliverer()}) != transitionApplied {
			h.channel.Cells().Despawn(inc)
			abortEnsure()
			return activationVerdict{kind: actBuildFailed, err: errors.New("platform: liveness publish rejected")}
		}
		h.redeliverOpenRequests(ctx, id)
		return activationVerdict{kind: actEmbodied, inc: inc}
	}
}

func (h *Home) reconcileActivation(ctx context.Context) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		h.logger.Error("platform.reconcile.authority_failed", "channel", string(h.channelID), "err", err)
		return
	}
	rt := h.channel.Cells()
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if _, retired := h.liveness.RetireIfVersionSkew(row.ID, row.CurrentDeclVersion); retired {
			rt.DespawnID(row.ID)
		}
		state, ok := h.liveness.WakeStanding(row.ID)
		if !ok {
			continue
		}
		if row.Placement.Kind == storespec.PlacementServer && state.Occ == occRunning {
			if _, live := rt.CurrentIncarnation(row.ID); !live {
				// Quiet teardown has no down edge. Repair the stale carrier level
				// before evaluating shouldRun so the ensure arm can rebuild it.
				// Token-guarded: a successor published between the read and this
				// repair makes the token stale and the repair a rejected no-op.
				_ = h.liveness.ObserveDown(row.ID, state.CarrierInc, false, false)
				state, _ = h.liveness.WakeStanding(row.ID)
			}
		}
		shouldRun := row.TIdle == 0 || state.Dirty || state.Restart || state.Occ != occNone
		if !shouldRun {
			continue
		}
		if row.Placement.Kind == storespec.PlacementDaemon {
			if state.Occ == occNone {
				_, _ = h.liveness.BeginEnsure(row.ID, row.CurrentDeclVersion)
			}
			continue
		}
		if row.Placement.Kind != storespec.PlacementServer || state.Occ == occRunning {
			continue
		}
		switch v := h.activateOne(ctx, row); v.kind {
		case actEmbodied, actAlreadyLive:
			h.clearReviveBackoff(row.ID)
			h.clearNoFactoryWarned(row.ID)
		case actAttached:
			h.clearNoFactoryWarned(row.ID)
		case actNoFactory:
			if h.firstNoFactoryWarning(row.ID) {
				h.logger.Warn("platform.reconcile.no_factory", "channel", string(h.channelID), "actor", string(row.ID))
			}
		case actSealed:
			return
		case actBuildFailed:
			h.logger.Error("platform.reconcile.build_failed", "channel", string(h.channelID), "actor", string(row.ID), "error", v.err)
		case actCancelled:
			return
		}
	}
}

// factoryFor is the single activation-dispatch point shared by the reconcile
// ring's 补臂 and homeReviver.EnsureLive: it maps an active control row to the
// ActorFactory that embodies it. A human member (Kind==KindHuman) resolves to the
// platform's OWN built-in human cell factory — user-domain supply is platform
// internal, so the composition resolver is never asked
// for it. Every other kind resolves through the required composition view.
// Kind is caller-held (rec.Kind), never re-answered.
func (h *Home) factoryFor(row storespec.ActorControlRow) (platform.ActorFactory, bool) {
	if row.Kind == actor.KindHuman {
		// 装配链 step② (gateway 期 v0.4.1 勘误: 槽随户籍准入 ensure): the per-identity
		// slot (在场与递交接头盒) is ensured HERE — the platform-authoritative membership→
		// embodiment dispatch (shared by the reconcile 补臂 and homeReviver.EnsureLive),
		// which covers BOTH a fresh Admit-poke AND a restart's durable re-read (a member
		// read from the store with no Admit call this run). This runs BEFORE the cell is
		// built, so the cell/factory step③ is a pure lookup — never a construction-path
		// self-ensure (装配授权走私). Idempotent; nil registry is a defensive no-op.
		h.ensureSubjectSlot(row.ID)
		return humanCellFactory(h, row.ID), true
	}
	return h.factories.LookupByClass(row.ID, row.Class, row.Config)
}

// recheckResult classifies a post-build straddle recheck (verifyPostBuild).
type recheckResult int

const (
	recheckOK    recheckResult = iota // still an active member — keep the build
	recheckGone                       // Ended / no longer active — build undone
	recheckStale                      // selected version/placement changed — retry from current
	recheckFault                      // registry lookup fault — build undone
)

// verifyPostBuild is the shared straddle-window closure for every eager LOCAL birth
// (the reconcile ring's 补臂 and homeReviver.EnsureLive, via activateOne): after
// SpawnIfAbsent mints inc, re-read the authority NOW and confirm id is still an
// active identity. The window it closes: a concurrent End landing
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
func (h *Home) verifyPostBuild(ctx context.Context, id actor.ActorID, selectedVersion int64, inc actorrt.Incarnation) (recheckResult, error) {
	row, ok, lerr := h.controlIndex.LookupActive(ctx, id)
	if ctx.Err() != nil {
		h.channel.Cells().Despawn(inc)
		return recheckFault, ctx.Err()
	}
	if lerr != nil {
		h.channel.Cells().Despawn(inc)
		return recheckFault, lerr
	}
	if !ok {
		h.channel.Cells().Despawn(inc)
		// 死 ID 槽级联清 (gateway 期 P1, mirror remove.go §级联删槽): factoryFor embodies a
		// human by the slot ensure step BEFORE this build (装配链 step②). A stale-rec 补臂/
		// EnsureLive whose Lookup predated a concurrent Home.Remove can therefore RE-create
		// the slot AFTER Remove's slot removal ran — resurrecting a dead id's
		// slot. Despawn alone (above)只 evicts the cell, not the slot, so close the笔 here
		// with the SAME idempotent cascade Remove uses (id is confirmed dead — 身份不可复活
		// mints a fresh id on any re-Admit, so this only ever targets THIS dead id, never a
		// live successor). A no-op for a non-human (no slot was ever ensured) and when
		// Remove already cleaned it.
		h.removeSubjectSlot(id)
		h.presenceFold.Forget(id)
		return recheckGone, nil
	}
	if row.CurrentDeclVersion != selectedVersion || row.Placement.Kind != storespec.PlacementServer {
		h.channel.Cells().Despawn(inc)
		return recheckStale, nil
	}
	return recheckOK, nil
}

// pokeReconcile posts a coalesced wake to the reconcile ring (non-blocking: a
// full buffer already carries the pending edge). No-op if the ticker goroutine
// has not launched yet (genesis is covered by the synchronous startup sweep).
func (h *Home) pokeReconcile() {
	if h.pokeCh == nil || h.disablePoke.Load() {
		return
	}
	select {
	case h.pokeCh <- struct{}{}:
	default:
	}
}
