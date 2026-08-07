package link

import (
	"errors"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var ErrInvalidPhysicalChild = errors.New("link: invalid physical child")

type RawActorArms struct {
	Pen       harness.Pen
	Access    accessdoor.ResourceAccessHandle
	State     accessdoor.AccessHandle
	Schedule  schedule.ScheduleHandle
	Lifecycle actorcaps.LifecycleHandle
}

type ActorStreamResource struct {
	Arms          RawActorArms
	Close         func() error
	Done          <-chan struct{}
	CancelRequest func(message.ID) error
	PublishObs    func(string, []byte) error
}
