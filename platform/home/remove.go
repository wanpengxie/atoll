package home

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ErrRemoveAnchor rejects Remove against the intrinsic system actor — the
// channel's structural anchor, never a member id ApplyMemberTransitions was
// ever meant to deregister. The daemon relay anchor needs no parallel guard
// here: it is a compute id, never a member id Remove's caller could pass.
var ErrRemoveAnchor = errors.New("platform: cannot remove the system anchor actor")

var ErrRestartAnchor = errors.New("platform: cannot restart the system anchor actor")

// Restart accepts a reconcile-driven restart: desired membership remains
// untouched, the current embodiment is killed, and the ring is poked to rebuild.
func (h *Home) Restart(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if id == actor.SystemActorID {
		return ErrRestartAnchor
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: Restart membership lookup: %w", err)
	}
	if !ok || !rec.IsActive() {
		return fmt.Errorf("platform: Restart requires an active member: %s", id)
	}
	h.channel.Cells().DespawnID(id)
	h.pokeReconcile()
	return nil
}

// Remove is identity-level termination's paved path: despawn-first + dereg
// cascade + double-tap. This is a COMPOSITION over already-built primitives
// (DespawnID, ApplyMemberTransitions) — NOT a single atomic operation spanning
// runtime and store: the schedule engine's Reviver can activate id between
// steps ① and ② (a Due snapshot already in flight when Remove starts), so the
// trailing double-tap at ③ closes that window. The Reviver's own post-build
// recheck (homeReviver.EnsureLive, S-P20) closes the mirror half — a build
// that starts after ① and lands after ③ re-reads the registry under its own
// fresh incarnation and self-undoes. Together the two halves close the window
// completely (owner 2026-07-03 拍定 A′): no third straddle case exists.
//
// desired 权威口径 (红线 5): Remove never touches desired/intent. If
// channel_actors still lists id, the next reconcile tick re-minting it is the
// CORRECT reconcile behaviour (same as killing a pod without deleting its
// Deployment) — the caller's obligation is to remove intent FIRST, then call
// Remove (asserted at the app call site, period 9).
func (h *Home) Remove(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if id == actor.SystemActorID {
		return ErrRemoveAnchor
	}
	// Capture the principal for the membership-change poke BEFORE the dereg cascade
	// (连接模型勘误期 §3.2 表②: ② ApplyMemberTransitions deregisters the registry row,
	// so PrincipalOf would fail after). best-effort: an already-gone id yields "" and
	// the poke is skipped (the resolver sweep is the正门).
	principal, _, _ := h.PrincipalOf(ctx, id)
	// ① despawn-first: kill any live embodiment (cell or attached port,
	// transport-neutral) before the dereg cascade below. false = no live
	// embodiment right now — not an error, dereg proceeds regardless.
	h.channel.Cells().DespawnID(id)
	// ② dereg cascade: state + timers + mirror event, one tx (actors.go). An
	// already-deregistered id is an idempotent no-op — no repeat mirror, nil err.
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{
		{ID: id, At: h.nowMs()},
	}); err != nil {
		return fmt.Errorf("platform: Remove dereg %s: %w", id, err)
	}
	// A4/C2: symmetric to Admit's platform.member.admitted (census.go) — P1
	// migration-pair rule, same posture (fires on every successful call,
	// idempotent retries included, matching admitted's own precedent).
	// MembershipControlPlane's frozen ApplyMemberTransitions signature (many
	// callers outside this package/cluster) carries back no changed/count
	// signal, so this line cannot condition on "did it actually cascade" or
	// name the state/timers/grants row counts — those land durably instead
	// on the system.actor.deregistered mirror payload's state_cleared/
	// timers_cleared/grants_cleared fields (actors.go), inspectable per event.
	h.logger.Info("platform.member.removed", "channel", string(h.channelID), "actor", string(id))
	// ③ double-tap: kill whatever the Reviver may have spawned in the window
	// between ① and ②'s commit (see the doc comment above).
	h.channel.Cells().DespawnID(id)
	// Presence归一清账 (gateway 期 S4, design §5.4 "Forget 证词账清洁边"): the
	// ring's削 is a quiet teardown with no down edge, so without this the removed
	// member's device presence would fold "online" forever. Two owner-side清账:
	// 级联删槽 (RemoveSubjectSlot — drop the slot from the registry and
	// revoke its layer-3 testimony to any observer) + Forget the presence fold row
	// (unknown 恒 = 无行). Attribution honesty: RemoveSubjectSlot/Forget are not
	// serialized against a concurrent re-Admit, but 身份不可复活 mints a FRESH id on
	// re-Admit, so this targets the dead id only; a residual race is at worst a
	// false-unknown (advisory-safe, never a false-online — 解绑永不训练 offline).
	// Timer rows are already cleared by the dereg cascade (clearTimersTx); the
	// scheduler's EnsureLive户籍拒 is the second line.
	h.RemoveSubjectSlot(id)
	h.presenceFold.Forget(id)
	h.reviveMu.Lock()
	delete(h.reviveLogAt, id)
	delete(h.reviveBackoff, id)
	h.reviveMu.Unlock()
	// Membership-change poke emit point (连接模型勘误期 §3.2 表②): the dereg cascade has
	// committed, so the subject that just lost membership must have their gateway
	// session re-resolve (drop the subscription + stop the stream). The assembly root
	// bridges this into the gateway's PokeHub → Gateway.Poke(principal); the read-side
	//每批 recheck is the correctness正门, this poke is pure及时性. nil sink / empty
	// principal → no-op.
	if h.onMembershipChange != nil && principal != "" {
		h.onMembershipChange(principal)
	}
	return nil
}
