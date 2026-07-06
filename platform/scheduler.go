package platform

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fireSink is the platform realisation of schedule.FireSink: the time engine's
// one non-ambient collaborator. It mints a fresh Pen per fire (Mint is cheap,
// same posture as the port emitSink) so the fired envelope is authored AS the
// timer's author through the full harness chain — the engine can never choose an
// author other than the row's author_id. The author's kind is resolved from the
// durable registry (identity-bind fires always have a member author — dereg
// cascades clear its timers in the same tx, so a fired identity timer's author is
// still a member); the kind rides the minted pen exactly as at admission, so a
// sender-kind consumer sees the same value a live cell's own write would carry.
type fireSink struct {
	minter   harness.Minter
	registry storespec.Registry
	rt       *actorrt.Runtime
	chID     channelpkg.ID
}

// Append translates harness.WriteResult into the FireSink tri-state contract: a
// naive `_, err := pen.Write(...); return err` would swallow a deterministic
// reject into a false nil and let the engine drop the fire silently — that
// failure mode is the entire reason this translation exists.
//
// G11 双神谕: kind is resolved by trying the incarnation-level oracle FIRST
// (rt.Stat — the live embodiments table), falling back to the identity-level
// oracle (the durable registry, G10-guarded by IsActive) only if no live
// embodiment answers. This order is not arbitrary: an incarnation-bind fire's
// author is IsLive-checked by the caller immediately before Append runs (a
// fork child is NEVER a durable member — it has no registry row at all), while
// an identity-bind fire's author was just revived-or-already-live by the
// engine's wake-first Reviver step, so it too is normally live by the time
// Append runs. Either way, a live embodiment's own kind is the freshest,
// authoritative answer and needs no registry hop; the registry fallback is
// what keeps a genuinely COLD identity fire (revived, then died again before
// Append — or a G10 dereg race) working, and what turns a fork child that died
// between its IsLive check and this call into a quiet, deterministic
// author_not_member drop (never a false live-embodiment read of a departed
// author's kind) rather than a resurrection.
func (s fireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	var kind actor.Kind
	if stat, ok := s.rt.Stat(author); ok {
		kind = stat.Kind
	} else {
		rec, ok, err := s.registry.Lookup(ctx, author)
		if err != nil {
			return err // transient: the engine leaves the row for the next tick.
		}
		if !ok || !rec.IsActive() {
			// The author is not a LIVE durable member — its kind must not be welded.
			//   !ok: a poison row whose author never existed (includes a fork child:
			//   it is never a durable member, so a dead fork child falls straight
			//   through to here — the quiet, deterministic drop G11's death-race
			//   guard relies on).
			//   !IsActive: the author was deregistered AFTER the engine snapshotted
			//   this fire as due but BEFORE Append ran. Deregister soft-removes the
			//   registry row and cascade-clears its timer rows in ONE tx, but a fire
			//   already snapshotted into the due set is in flight against the
			//   pre-dereg view — the deadline-boundary race (cf. Engine.cancel's
			//   declared window). Welding a pen here would append truth authored by
			//   a member who is already gone.
			// Both are deterministic rejects (the engine disposes the row), never a
			// hot retry loop — a soft-dereg is monotonic, it never reverts to active.
			return schedule.FireRejected{Reason: "author_not_member", Detail: string(author)}
		}
		kind = rec.Kind
	}
	pen := s.minter.Mint(author, kind, s.chID)
	res, err := pen.Write(ctx, env)
	if err != nil {
		return err // transient Go error (store/transport fault): retain and retry.
	}
	if res.Accepted() {
		return nil
	}
	if res.RejectReason == harness.HarnessIDDuplicateConflict && res.MessageID == env.ID {
		return schedule.ErrDuplicateFire
	}
	return schedule.FireRejected{Reason: string(res.RejectReason), Detail: res.RejectDetail}
}

var _ schedule.FireSink = fireSink{}

// homeReviver is the platform realisation of schedule.Reviver: the identity-timer
// activation seam. A wake with no live actor is the NORMAL restart path (overdue
// fires run before the eager reconcile ring re-mints the always-on set), and
// append has no backfill — so firing without reviving first would silently lose
// the wake. EnsureLive resolves the factory through the SAME builder table
// activation uses, welds caps at the platform seam (buildCaps), and mints the
// embodiment via SpawnIfAbsent (idempotent for an already-live author — the CAS
// discards the freshly-built shell if the id is already occupied). It never opens
// a new mint path: revival goes through the runtime's existing SpawnIfAbsent CAS.
type homeReviver struct{ h *Home }

// EnsureLive activates id if absent. The two-class error contract mirrors
// FireSink: a structurally unrevivable author (no builder wired, class not in the
// table, id not a durable member) is a ReviveRejected poison row the engine
// disposes of; a transient registry fault is a plain error the engine retries.
func (r homeReviver) EnsureLive(ctx context.Context, id actor.ActorID) error {
	h := r.h
	// Already live: an idempotent no-op — return BEFORE the builder gate. Liveness
	// is checked first because the builder is only needed to activate an ABSENT
	// author; an identity-timer fire for an already-embodied author must succeed
	// even when no builder is wired (a nil-builder home is legal — it simply has no
	// eager/fork activation). Gating a live author on the builder would turn a
	// legitimate wake into a ReviveRejected{no_builder} poison verdict, and the
	// engine would delete the timer row (silently losing a live author's timer).
	if _, ok := h.channel.Cells().CurrentIncarnation(id); ok {
		return nil
	}
	// S6 (§10.13 推导2/3): consult the placement fact BEFORE the class table —
	// AND before the nil-builder gate. An attached author's activation authority
	// is its daemon's own feasible check, not home's: a nil-builder home hosting
	// an attached author's identity timer must classify the wake as transient
	// (S6), never as ReviveRejected{no_builder} — the builder is only ever needed
	// to activate a HOME-placed absent author, so gating on it before placement
	// classification would poison-delete a live daemon actor's timer row. A
	// registry fault or an attached Host are BOTH plain (transient) errors, never
	// ReviveRejected: the row is retained and retried next tick, so the SAME wake
	// fires normally once the fault clears or the author is no longer attached
	// (poisoning here would turn a placement fact into a false identity-death
	// verdict).
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: revive lookup %s: %w", id, err) // transient
	}
	if !ok || !rec.IsActive() {
		// !ok: never a member. !IsActive: deregistered after this wake was
		// snapshotted as due but before revive ran (the same deadline-boundary race
		// FireSink.Append closes) — reviving would resurrect a zombie cell for an
		// identity the dereg cascade already tore down (state + timers cleared).
		// Both are the deterministic unrevivable class: dispose the row, never
		// hot-retry (soft-dereg is monotonic, it never reverts to active).
		return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
	}
	if rec.Host != "" {
		h.logReviveAttached(id, rec.Host)
		return fmt.Errorf("platform: revive %s: attached to host %q", id, rec.Host) // transient
	}
	// Home-placed, absent: resolve through factoryFor (human → the platform's own
	// built-in cell factory, needing no builder; others → the组合域 builder, now
	// load-bearing). A human revive therefore never depends on a wired builder (a
	// nil-builder home still revives its identity-timer-bearing humans); a non-human
	// miss splits into the two structurally-unrevivable classes the engine disposes.
	kind := rec.Kind
	factory, ok := h.factoryFor(rec)
	if !ok {
		if h.builder == nil {
			return schedule.ReviveRejected{Reason: "no_builder", Detail: string(id)}
		}
		return schedule.ReviveRejected{Reason: "class_not_found", Detail: string(id)}
	}
	// straddleHook (test-only, nil in production): fires AFTER the Lookup above
	// passed but BEFORE SpawnIfAbsent below — the exact window Home.Remove's
	// double-tap closure argument (S-P20) requires a test able to park a build
	// in. Zero cost when nil.
	if h.reviverStraddleHook != nil {
		h.reviverStraddleHook()
	}
	// SpawnIfAbsent is the idempotent CAS: an already-live author is a no-op
	// (ok=false, shell discarded), so EnsureLive satisfies its idempotency contract
	// without a separate liveness pre-check.
	inc, built := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return build(h.buildCaps(id, kind, inc), h.hooks(), factory)
	})
	if !built {
		return nil
	}
	// Post-build recheck (S-P20 拍定 A′, Remove's straddle-window closure other
	// half): the id could have been Remove'd (dereg) or daemon-attached (Host stamp)
	// BETWEEN the Lookup above and this build landing (Remove's own double-tap only
	// guards its own before/after, not a build that starts after Remove's ① and
	// finishes after its ③). verifyPostBuild re-reads under the fresh inc and undoes
	// the build (pointer-guarded Despawn) on any non-OK outcome; here we only map its
	// verdict onto the engine's two-class contract:
	//   - fault: transient — the recheck itself failed, so the freshly-built cell is
	//     unconfirmed (an unverified build is not a validated live one); retry next tick.
	//   - gone: ReviveRejected{not_a_member} — the dereg cascade already tore down its
	//     state + timers; reviving would resurrect a zombie.
	//   - attached: transient — home is not the placement authority for an attached
	//     identity; the SAME wake fires normally once the port installs (already-live
	//     fast path) or the author detaches back home.
	rec2, res, lerr := h.verifyPostBuild(ctx, id, inc)
	switch res {
	case recheckFault:
		return fmt.Errorf("platform: revive post-build recheck %s: %w", id, lerr)
	case recheckGone:
		return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
	case recheckAttached:
		h.logReviveAttached(id, rec2.Host)
		return fmt.Errorf("platform: revive %s: attached to host %q after build", id, rec2.Host) // transient
	}
	return nil
}

var _ schedule.Reviver = homeReviver{}
