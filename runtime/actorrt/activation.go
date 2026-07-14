package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// DesiredMember is one row of the desired truth: a durable member the control
// plane wants managed, with the kind the reconcile loop passes straight to the
// pen weld (kind is caller-held here, same rule as ForkSpec.Kind — the Builder
// is never asked to re-answer it).
type DesiredMember struct {
	ID   actor.ActorID
	Kind actor.Kind
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
