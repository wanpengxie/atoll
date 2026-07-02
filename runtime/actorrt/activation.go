package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Lifecycle is a desired member's activation intent — the closed set of ways a
// durable member wants its liveness managed. It is content the control plane
// declares per member, not a new mint type: both values flow through the same
// mint.
type Lifecycle string

const (
	// LifecycleAlwaysOn: the eager reconcile ring (not yet built; this
	// contract's shape has already shipped) will keep a live incarnation up
	// whenever this member appears in desired.
	LifecycleAlwaysOn Lifecycle = "always_on"
	// LifecycleLazy: no eager revival; activation on demand at the delivery
	// seam (member-but-no-live → activate) — deferred for now, kept in the
	// closed set so desired declarations don't need a schema change when the
	// lazy entry lands.
	LifecycleLazy Lifecycle = "lazy"
)

// DesiredMember is one row of the desired truth: a durable member the control
// plane wants managed, with the kind the reconcile loop passes straight to the
// pen weld (kind is caller-held here, same rule as ForkSpec.Kind — the Builder
// is never asked to re-answer it).
type DesiredMember struct {
	ID        actor.ActorID
	Kind      actor.Kind
	Lifecycle Lifecycle
}

// DesiredSource is the reconcile loop's read of desired: the intent half of
// the desired−actual diff whose actual half is LiveIDs(). The interface is
// substrate-defined; the IMPLEMENTATION is injected by the app assembly root
// and must yield only confirmed durable members — activation skips membership
// apply (SpawnIfAbsent, never Home.Spawn), so feeding it a raw intent row that
// never landed in durable membership would mint an actor with no membership
// at all. Substrate does not know or touch the table behind this.
type DesiredSource interface {
	Members(ctx context.Context) ([]DesiredMember, error)
}
