package platform

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
	if id == actor.SystemActorID {
		return ErrRemoveAnchor
	}
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
	// ③ double-tap: kill whatever the Reviver may have spawned in the window
	// between ① and ②'s commit (see the doc comment above).
	h.channel.Cells().DespawnID(id)
	// H5: id is no longer a member — its obs registration must not survive the
	// dereg (WatchObs is append-only; leaving the entry would leak across a
	// future re-admission of the same id).
	h.unwatchObs(id)
	// A removed subject's shared Caller must not outlive its membership: stop its
	// pending timers (no死后 unanswered_timeout write through the裸 pen) and drop
	// the by-id index entry. A no-op for a non-human id (no caller was minted).
	h.stopHumanCaller(id)
	return nil
}
