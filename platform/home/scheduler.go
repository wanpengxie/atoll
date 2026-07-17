package home

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const reviveBuildBackoffBase = time.Second
const reviveBuildBackoffMax = 5 * time.Minute

type reviveBackoffEntry struct {
	failures uint
	next     time.Time
}

func (h *Home) clearReviveBackoff(id actor.ActorID) {
	h.reviveMu.Lock()
	delete(h.reviveBackoff, id)
	h.reviveMu.Unlock()
}

// backoffGate reports whether id is currently held off from a build attempt by a
// not-yet-elapsed build-failure backoff, and until when. Both activation paths —
// homeReviver.EnsureLive and reconcileActivation's 补臂 — consult it BEFORE a
// SpawnIfAbsent build so a persistently failing actor retries at the
// CrashLoopBackOff pace instead of being re-hammered every wake / tick / poke.
func (h *Home) backoffGate(id actor.ActorID, now time.Time) (until time.Time, held bool) {
	h.reviveMu.Lock()
	entry := h.reviveBackoff[id]
	h.reviveMu.Unlock()
	if !entry.next.IsZero() && now.Before(entry.next) {
		return entry.next, true
	}
	return time.Time{}, false
}

// recordBuildFailure advances id's activation-failure backoff one step:
// deterministic factory failures and involuntary local-body deaths share the
// same exponential ladder. A successful carrier publication clears it.
func (h *Home) recordBuildFailure(id actor.ActorID, now time.Time) {
	h.reviveMu.Lock()
	entry := h.reviveBackoff[id]
	entry.failures++
	delay := reviveBuildBackoffBase << min(entry.failures-1, 8)
	if delay > reviveBuildBackoffMax {
		delay = reviveBuildBackoffMax
	}
	entry.next = now.Add(delay)
	h.reviveBackoff[id] = entry
	h.reviveMu.Unlock()
	h.pokeReconcileAfter(delay)
}

func (h *Home) pokeReconcileAfter(delay time.Duration) {
	if delay <= 0 {
		h.pokeReconcile()
		return
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		if h.closeDone == nil {
			<-timer.C
			h.pokeReconcile()
			return
		}
		select {
		case <-timer.C:
			h.pokeReconcile()
		case <-h.closeDone:
		}
	}()
}

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
	minter    harness.Minter
	authority storespec.ActorAuthority
	rt        *actorrt.Runtime
	chID      channelpkg.ID
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
// fork child is run-world only — it has no durable identity row), while
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
		rec, ok, err := s.authority.LookupActive(ctx, author)
		if err != nil {
			return err // transient: the engine leaves the row for the next tick.
		}
		if !ok {
			// The author is not a live identity — its kind must not be welded.
			//   !ok: a poison row whose author never existed (includes a fork child:
			//   it is never durable, so a dead fork child falls straight
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
	row, ok, authorityErr := s.authority.LookupActive(ctx, author)
	if authorityErr != nil {
		return authorityErr
	}
	if !ok {
		return schedule.FireRejected{Reason: "author_not_member", Detail: string(author)}
	}
	pen := s.minter.Mint(author, kind, s.chID, row.CurrentDeclVersion)
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
// table, id no longer active) is a ReviveRejected poison row the engine
// disposes of; a transient registry fault is a plain error the engine retries.
// Everything from the placement filter through the post-build straddle recheck
// runs inside the shared activateOne core (§1.1); this is its reviver 翻译器
// (§1.7): the ONLY vocabulary it speaks back to the schedule engine is
// ReviveRejected (the poison class) vs a plain transient error, plus the two
// account side effects (clearReviveBackoff / the throttled attached log) each
// word's row in §1.7 names.
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
	// Single-point Lookup + its transient semantics stay OUTSIDE the core (§1.8):
	// a registry fault here is a plain transient error. !ok (never a member at
	// all — no row to even hand the core) is handled HERE, never by calling
	// IsActive on a zero-value Record (whose zero DeregisteredAt would read as
	// active); an EXISTING-but-deregistered row is instead handed to
	// activateOne, whose own ①IsActive 防御复判 is this path's REAL (not merely
	// defensive) not-a-member detector — DoD §4.2's 3-site IsActive() closure
	// counts on EnsureLive itself making no separate IsActive() call.
	row, ok, err := h.controlIndex.LookupActive(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: revive lookup %s: %w", id, err) // transient
	}
	if !ok {
		return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
	}
	switch v := h.activateOne(ctx, row); v.kind {
	case actEmbodied:
		h.clearReviveBackoff(id)
		return nil
	case actAlreadyLive:
		// Kept as its own arm (not merged with actEmbodied above) so the four
		// clearReviveBackoff write sites §1.4 enumerates stay individually
		// visible: ring fast-path, ring削臂, and the reviver's two verdicts.
		h.clearReviveBackoff(id)
		return nil
	case actNotMember: // the deregistered-row case: caught HERE by activateOne's own IsActive check
		return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
	case actAttached:
		// S6 (§10.13 推导2/3): home is not the placement authority for an attached
		// identity — the SAME wake fires normally once the port installs
		// (already-live fast path above) or the author detaches back home.
		h.logReviveAttached(id, v.host)
		_ = h.liveness.MarkFiredWake(id)
		h.pokeReconcile()
		return fmt.Errorf("platform: revive %s: attached to host %q", id, v.host) // transient
	case actBackoffHeld:
		return fmt.Errorf("platform: revive %s: build backoff until %s", id, v.until)
	case actNoFactory:
		return schedule.ReviveRejected{Reason: v.reason, Detail: string(id)}
	case actSealed:
		h.logger.Info("platform.revive.runtime_sealed", "channel", string(h.channelID), "actor", string(id))
		return v.err // transient
	case actBuildFailed:
		var buildFailure *actorrt.BuildFailure
		if errors.As(v.err, &buildFailure) {
			if panicErr, ok := buildFailure.PanicValue.(error); ok && errors.Is(panicErr, accessdoor.ErrStateHandleUnavailable) {
				return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
			}
		}
		h.logger.Error("platform.revive.build_failed", "channel", string(h.channelID), "actor", string(id), "error", v.err)
		return v.err // transient
	case actCancelled:
		// §1.9②: net behavior unchanged (the freshly built cell is undone inside
		// the core either way) — only the error wording changes, from the former
		// recheckFault wrap to its own Cancelled word (this branch used to be
		// unreachable from EnsureLive's own control flow pre-extraction; sharing
		// the core's ctx gate is what newly routes a same-window cancel here —
		// §1.9①'s three flipped cross-cells).
		return fmt.Errorf("platform: revive %s: cancelled post-spawn: %w", id, ctx.Err())
	case actRecheckFault:
		return fmt.Errorf("platform: revive post-build recheck %s: %w", id, v.err)
	case actRecheckGone:
		// The dereg cascade already tore down state + timers; reviving would
		// resurrect a zombie. #24 (§0.5-A): no actAttached-shaped case exists
		// here — a concurrent daemon attach in this same window is a DIFFERENT
		// race, closed by Attach's own replace semantics, never by this recheck
		// (see verifyPostBuild's doc).
		return schedule.ReviveRejected{Reason: "not_a_member", Detail: string(id)}
	case actRecheckStale:
		return fmt.Errorf("platform: revive %s: declaration changed during build", id)
	}
	return nil // unreachable: the switch above is exhaustive over activationOutcome
}

var _ schedule.Reviver = homeReviver{}
