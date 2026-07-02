package actorrt

import "github.com/wanpengxie/atoll/protocol/actor"

// HostRef names the physical host a new incarnation is placed on. It is
// incarnation-level and opaque from actorrt's own vantage — actorrt does not
// interpret or compare its contents, only carries it through
// Placement.Place's return value.
type HostRef string

// Placement decides which host a new activity (fork or reconcile-driven
// activation) starts on. It answers a physical, incarnation-level question —
// orthogonal to membership (durable, identity-level, intent-defined): a
// placement change (re-activating on a different host) never touches
// membership, and being a member never implies a live incarnation exists.
//
// This ships shape only — a single fixed-home identity implementation lives
// in platform (platform.SinglePlacement). Multi-home selection is deferred
// until a second home exists; the value of shaping Placement now is that
// fork/activation call sites can later route through Place() explicitly and
// additively swap implementations when multi-home arrives, without changing
// call sites.
type Placement interface {
	Place(id actor.ActorID, kind actor.Kind) (HostRef, error)
}
