package platform

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

type DaemonMembrane struct {
	Generation      uint64
	ChannelName     string
	Ingress         remoteingress.RemoteIngress
	AuthorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	AttachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	BindingDown     func(actor.ActorID, actorhost.Binding)
	Observe         func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding, actorrt.ObsKind, actorrt.ObsValue)
	ObserveDown     func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding)
	CancelRequest   func(actor.ActorID, message.ID)
	ResolveTarget   func(string) (actor.ActorID, error)
	Plan            func(context.Context, string) ([]PlanActor, error)
	IsBound         func(context.Context, string) (bool, error)
}

type DaemonFileInfo struct {
	Path     string
	NodeType accessdoor.FileNodeType
	Size     int64
	// ModifiedAt is Unix milliseconds, zero when the device reported none.
	ModifiedAt int64
}

type DaemonRoutes interface {
	PokePlan(string, string)
	FileCreate(context.Context, string, string, string, accessdoor.FileNodeType) error
	FileDelete(context.Context, string, string, string) error
	FileStat(context.Context, string, string, string) (DaemonFileInfo, bool, error)
	FileList(context.Context, string, string, string, int, string) ([]DaemonFileInfo, string, error)
	AttachedDaemons(string) []string
	LaneAttached(string, string) bool
	// LaneWorkspace answers where a daemon keeps a channel's directory on its
	// own filesystem. The access plane needs it to turn a device-local absolute
	// path into the channel-relative one it addresses by, and the device is the
	// only side that knows $ATOLL_HOME.
	LaneWorkspace(context.Context, string, string) (string, bool, error)
}
