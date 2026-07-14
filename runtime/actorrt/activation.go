package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Lifecycle is a desired member's activation intent — the closed set of ways a
// durable member wants its liveness managed. It is content the control plane
// declares per member, not a new mint type: the single value flows through the
// same mint as everything else (H4: the LifecycleLazy ghost value — a public
// promise to DesiredSource implementers with no corresponding machine
// semantics, a half-built-result — was ripped from the set; additive to bring
// back when a real second lifecycle semantics exists).
type Lifecycle string

const (
	// LifecycleAlwaysOn: the eager reconcile ring keeps a live incarnation up
	// whenever this member appears in desired (home.Home.reconcileActivation /
	// computeRing.reconcile).
	LifecycleAlwaysOn Lifecycle = "always_on"
)

// DesiredMember is one row of the desired truth: a durable member the control
// plane wants managed, with the kind the reconcile loop passes straight to the
// pen weld (kind is caller-held here, same rule as ForkSpec.Kind — the Builder
// is never asked to re-answer it).
type DesiredMember struct {
	ID        actor.ActorID
	Kind      actor.Kind
	Lifecycle Lifecycle
	// Epoch identifies the desired incarnation. A changed epoch is not a
	// metadata refresh: reconcile must tear down the body built for the old
	// value and build a new one, recording the epoch only after build succeeds.
	Epoch int64
}

// DesiredSource is the reconcile loop's read of desired: the intent half of
// the desired−actual diff whose actual half is LiveIDs(). The interface is
// substrate-defined; the IMPLEMENTATION is injected by the app assembly root
// and must yield only confirmed durable members — activation skips membership
// apply (SpawnIfAbsent, never membership admission), so feeding it a raw intent row that
// never landed in durable membership would mint an actor with no membership
// at all. Substrate does not know or touch the table behind this.
type DesiredSource interface {
	Members(ctx context.Context) ([]DesiredMember, error)
}
