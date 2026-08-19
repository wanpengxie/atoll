package platform

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
	Path string
	Size int64
}

type DaemonRoutes interface {
	PokePlan(string, string)
	FileCreate(context.Context, string, string, string) error
	FileDelete(context.Context, string, string, string) error
	FileStat(context.Context, string, string, string) (DaemonFileInfo, bool, error)
	FileList(context.Context, string, string, string) ([]DaemonFileInfo, error)
	AttachedDaemons(string) []string
	LaneAttached(string, string) bool
}
