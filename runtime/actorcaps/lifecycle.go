package actorcaps

import (
	"context"
)

// EndSelfRequest carries the actor-selected diagnostic reason for ending
// itself. Caller identity and the physical attempt are welded into the handle,
// never supplied by this payload.
type EndSelfRequest struct {
	Reason string
}

// LifecycleHandle is the managed actor's logical lifecycle capability. Its
// implementations weld caller ActorID and AttemptKey at construction.
type LifecycleHandle interface {
	EndSelf(context.Context, EndSelfRequest) error
}
