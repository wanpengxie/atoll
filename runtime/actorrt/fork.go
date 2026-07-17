package actorrt

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrParentNotLive = errors.New("actorrt: parent not live")
	ErrNotOwner      = errors.New("actorrt: child not owned by parent")
)

// ForkSpec is the transport-neutral, serialisable child declaration. Admission
// and kinship authority live in Home; Runtime has no child-ownership table.
type ForkSpec struct {
	Kind      actor.Kind
	Class     string
	NameHint  string
	Config    json.RawMessage
	Placement *storespec.Placement
}

// LifecycleHandle is the sole actor lifecycle capability, welded to the
// caller's incarnation and birth declaration version by its implementation.
type LifecycleHandle interface {
	Fork(ctx context.Context, spec ForkSpec) (actor.ActorID, error)
	DespawnChild(ctx context.Context, childID actor.ActorID, reason string) error
	EndSelf(ctx context.Context) error
}
