package platform

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

// DaemonMembrane is the complete, generation-scoped capability bundle a
// channel publishes to the realm daemon host. It contains no transport.
type DaemonMembrane struct {
	Generation uint64

	Ingress         remoteingress.RemoteIngress
	AuthorizeAttach func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error
	AttachBinding   func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error
	BindingDown     func(actor.ActorID, actorhost.Binding)
	Observe         func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding, actorrt.ObsKind, actorrt.ObsValue)
	ObserveDown     func(actor.ActorID, actorhost.AttemptKey, actorhost.Binding)
	CancelRequest   func(actor.ActorID, message.ID)
	Plan            func(context.Context, string) ([]PlanActor, error)
	IsBound         func(context.Context, string) (bool, error)
	Storage         DaemonStorageAuthority
}

// DaemonStorageAuthority is the server-side authority used by one lane.
type DaemonStorageAuthority interface {
	Committed(context.Context, string, string) (bool, bool, error)
	ReclaimAck(context.Context, string, string) (bool, error)
	ReconcilePull(context.Context, string, []string) ([]StorageResourceCoord, []StorageReservationCoord, []StorageTombstoneCoord, error)
}

type StorageResourceCoord struct {
	Coord string `json:"coord"`
}

type StorageReservationCoord struct {
	ReservationID string `json:"reservation_id"`
	Coord         string `json:"coord"`
}

type StorageTombstoneCoord struct {
	TombstoneID string `json:"tombstone_id"`
	Coord       string `json:"coord"`
}

// DaemonRoutes is the transport face injected into Homes. All coordinates are
// explicit; implementations must treat an unavailable route as an error or an
// empty candidate set, never as a binding verdict.
type DaemonRoutes interface {
	PokePlan(string, string)
	SendAlloc(context.Context, string, string, string, bool) error
	SendReclaim(context.Context, string, string, string) error
	OpenTransfer(context.Context, string, string, string, access.Operation, string) (string, error)
	AttachedDaemons(string) []string
	LaneAttached(string, string) bool
	RetireLane(string, string)
}
