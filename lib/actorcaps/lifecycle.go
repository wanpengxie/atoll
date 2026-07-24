package actorcaps

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ForkSpec is the actor-facing declaration of a child. Placement uses the
// public protocol DTO; store normalization belongs to the control plane.
type ForkSpec struct {
	Kind      actor.Kind
	Class     string
	NameHint  string
	Config    json.RawMessage
	Placement *channel.Placement
}

// EndSelfRequest carries the actor-selected diagnostic reason for ending
// itself. Caller identity and the physical attempt are welded into the handle,
// never supplied by this payload.
type EndSelfRequest struct {
	Reason string
}

// LifecycleHandle is the managed actor's logical lifecycle capability. Its
// implementations weld caller ActorID and AttemptKey at construction.
//
// RequestID identifies the Fork operation for retry/read-back; it is not a
// physical delivery or incarnation identity.
type LifecycleHandle interface {
	Fork(context.Context, message.ID, ForkSpec) (actor.ActorID, error)
	EndSelf(context.Context, EndSelfRequest) error
}
