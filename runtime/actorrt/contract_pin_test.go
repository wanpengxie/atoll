package actorrt

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Shape-pinning for the contract-first activation/placement symbols
// (DesiredSource / DesiredMember / Lifecycle / Placement / HostRef): the eager
// reconcile ring (platform.Home.reconcileActivation / computeRing.reconcile)
// and multi-home placement consume these; these COMPILE-TIME assertions are
// the only thing preventing a refactor from silently deforming the pinned
// contract shapes. A signature change here must be a conscious contract
// revision, not a drive-by.
func TestActivationPlacementContractShapes(t *testing.T) {
	// DesiredSource: Members(ctx) ([]DesiredMember, error).
	var _ func(DesiredSource, context.Context) ([]DesiredMember, error) = DesiredSource.Members

	// DesiredMember carries exactly {identity, protocol classification,
	// lifecycle level} — the desired-state row the reconcile diff reads.
	_ = DesiredMember{ID: actor.ActorID("a"), Kind: actor.KindAgent, Lifecycle: LifecycleAlwaysOn}

	// Lifecycle closed set: one level today (additive room for more).
	if LifecycleAlwaysOn == "" {
		t.Fatal("lifecycle const must be non-empty")
	}

	// Placement: Place(id, kind) (HostRef, error).
	var _ func(Placement, actor.ActorID, actor.Kind) (HostRef, error) = Placement.Place
}
