package link

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type TargetResolver interface {
	ResolveTarget(context.Context, string) (actor.ActorID, error)
}

var ErrInvalidPhysicalChild = errors.New("link: invalid physical child")

type RawActorArms struct {
	Pen       harness.Pen
	Access    accessdoor.ResourceAccessHandle
	State     accessdoor.AccessHandle
	Schedule  schedule.ScheduleHandle
	Lifecycle actorcaps.LifecycleHandle
	Target    TargetResolver
}

type ActorStreamResource struct {
	Arms          RawActorArms
	Close         func() error
	Done          <-chan struct{}
	CancelRequest func(message.ID) error
	PublishObs    func(string, []byte) error
}
