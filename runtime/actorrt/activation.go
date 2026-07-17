package actorrt

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// DesiredMember is one row of execution intent projected by Home, with the kind
// the reconcile loop passes straight to the
// pen weld (kind is caller-held here, same rule as ForkSpec.Kind — the Builder
// is never asked to re-answer it).
type DesiredMember struct {
	ID   actor.ActorID
	Kind actor.Kind
	// Version identifies the selected declaration incarnation. A change is not a
	// metadata refresh: reconcile must tear down the body built for the old
	// value and build a new one, recording the version only after build succeeds.
	Version      int64
	IdleTimeout  time.Duration
	EnsureTicket string
}

// DesiredSource is the reconcile loop's read of desired: the intent half of
// the desired−actual diff whose actual half is LiveIDs(). The interface is
// substrate-defined; the IMPLEMENTATION is injected by the app assembly root
// and must yield only identities already admitted by their world's authority.
// SpawnIfAbsent never performs identity admission. Substrate does not know or
// touch the source behind this projection.
type DesiredSource interface {
	Members(ctx context.Context) ([]DesiredMember, error)
}
