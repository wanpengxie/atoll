package capauth

import "github.com/wanpengxie/atoll/protocol/actor"

// Authority is an opaque, already-welded admission capability. Implementations
// decide whether it is identity-level (A) or run-level (A/G); capability
// organs only invoke the one complete verdict at their real entry point.
type Authority interface {
	ActorID() actor.ActorID
	Admit() error
}
