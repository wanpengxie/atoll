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
	chID     channelpkg.ID
}

// Append translates harness.WriteResult into the FireSink tri-state contract: a
// naive `_, err := pen.Write(...); return err` would swallow a deterministic
// reject into a false nil and let the engine drop the fire silently — that
// failure mode is the entire reason this translation exists.
func (s fireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	rec, ok, err := s.registry.Lookup(ctx, author)
	if err != nil {
		return err // transient: the engine leaves the row for the next tick.
	}
	if !ok || !rec.IsActive() {
		// The author is not a LIVE durable member — its kind must not be welded.
		//   !ok: a poison row whose author never existed.
		//   !IsActive: the author was deregistered AFTER the engine snapshotted this
		//   fire as due but BEFORE Append ran. Deregister soft-removes the registry
		//   row and cascade-clears its timer rows in ONE tx, but a fire already
		//   snapshotted into the due set is in flight against the pre-dereg view —
		//   the deadline-boundary race (cf. Engine.cancel's declared window). Welding
		//   a pen here would append truth authored by a member who is already gone.
		// Both are deterministic rejects (the engine disposes the row), never a hot
		// retry loop — a soft-dereg is monotonic, it never reverts to active.
		return schedule.FireRejected{Reason: "author_not_member", Detail: string(author)}
	}
	pen := s.minter.Mint(author, rec.Kind, s.chID)
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
	// Home-placed, absent: only NOW is the builder load-bearing.
	if h.builder == nil {
		return schedule.ReviveRejected{Reason: "no_builder", Detail: string(id)}
	}
	factory, ok := h.builder.Lookup(id)
	if !ok {
		return schedule.ReviveRejected{Reason: "class_not_found", Detail: string(id)}
	}
	kind := rec.Kind
	// SpawnIfAbsent is the idempotent CAS: an already-live author is a no-op
	// (ok=false, shell discarded), so EnsureLive satisfies its idempotency contract
	// without a separate liveness pre-check.
	h.channel.Cells().SpawnIfAbsent(id, func(inc actorrt.Incarnation) actorrt.Actor {
		return factory(h.buildCaps(id, kind, inc))
	})
	return nil
}

var _ schedule.Reviver = homeReviver{}
