package actorrt

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Shape-pinning for the contract-first activation symbols (DesiredSource /
// DesiredMember): the eager reconcile ring
// (home.Home.reconcileActivation / computeRing.reconcile) consumes these;
// these COMPILE-TIME assertions are the only thing preventing a refactor from
// silently deforming the pinned contract shapes. A signature change here must
// be a conscious contract revision, not a drive-by.
func TestActivationContractShapes(t *testing.T) {
	// DesiredSource: Members(ctx) ([]DesiredMember, error).
	var _ func(DesiredSource, context.Context) ([]DesiredMember, error) = DesiredSource.Members

	// DesiredMember carries identity, protocol classification and incarnation
	// epoch — the desired-state row the reconcile diff reads.
	_ = DesiredMember{ID: actor.ActorID("a"), Kind: actor.KindAgent, Epoch: 1}
}
