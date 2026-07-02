package actorrt

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Shape-pinning for the contract-first activation/placement symbols
// (DesiredSource / DesiredMember / Lifecycle / Placement / HostRef). Their
// consumers arrive with the eager reconcile ring and multi-home placement;
// until then these COMPILE-TIME assertions are the only thing preventing a
// refactor from silently deforming the pinned contract shapes (Lifecycle
// is also the anchor referenced by the lazy timer entry point). A signature
// change here must be a conscious contract revision, not a drive-by.
func TestActivationPlacementContractShapes(t *testing.T) {
	// DesiredSource: Members(ctx) ([]DesiredMember, error).
	var _ func(DesiredSource, context.Context) ([]DesiredMember, error) = DesiredSource.Members

	// DesiredMember carries exactly {identity, protocol classification,
	// lifecycle level} — the desired-state row the reconcile diff reads.
	_ = DesiredMember{ID: actor.ActorID("a"), Kind: actor.KindAgent, Lifecycle: LifecycleAlwaysOn}

	// Lifecycle closed set: two levels.
	for _, l := range []Lifecycle{LifecycleAlwaysOn, LifecycleLazy} {
		if l == "" {
			t.Fatal("lifecycle const must be non-empty")
		}
	}

	// Placement: Place(id, kind) (HostRef, error).
	var _ func(Placement, actor.ActorID, actor.Kind) (HostRef, error) = Placement.Place
}
