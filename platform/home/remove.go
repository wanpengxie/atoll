package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// ErrRemoveAnchor rejects Remove against the intrinsic system actor — the
// channel's structural anchor, never a tenant identity subject to End
// ever meant to deregister. The daemon relay anchor needs no parallel guard
// here: it is a compute id, never a member id Remove's caller could pass.
var ErrRemoveAnchor = errors.New("platform: cannot remove the system anchor actor")

// Remove is identity-level termination's paved path: despawn-first + dereg
// cascade + double-tap. This is a COMPOSITION over already-built primitives
// (carrier retirement, EndCascade) — not a single atomic operation spanning
// runtime and store: the schedule engine's Reviver can activate id between
// steps ① and ② (a Due snapshot already in flight when Remove starts), so the
// trailing double-tap at ③ closes that window. The Reviver's own post-build
// recheck (homeReviver.EnsureLive, S-P20) closes the mirror half — a build
// that starts after ① and lands after ③ re-reads the registry under its own
// fresh incarnation and self-undoes. Together the two halves close the window
// completely (owner 2026-07-03 拍定 A′): no third straddle case exists.
//
// desired 权威口径 (红线 5): Remove never touches desired/intent. If
// channel composition still lists id, the next reconcile tick re-minting it is the
// CORRECT reconcile behaviour (same as killing a pod without deleting its
// Deployment) — the caller's obligation is to remove intent FIRST, then call
// Remove (asserted at the app call site, period 9).
func (h *Home) remove(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if id == actor.SystemActorID {
		return ErrRemoveAnchor
	}
	err := h.systemEndHandle().End(ctx, id, "removed")
	if err == nil {
		h.logger.Info("platform.member.removed", "channel", string(h.channelID), "actor", string(id))
	}
	return err
}
