package platform

import (
	"github.com/wanpengxie/atoll/protocol/actor"
)

// ActorDecl is the registry-to-host build result shared by Server and daemon
// composition roots, so it remains on the platform root membrane.

// ActorDecl declares one managed actor body. Factory is the ActorFactory both
// composition roots use. Server bodies receive direct capabilities; daemon
// bodies receive five stable facades backed by one exact OutboundSlot. Physical
// reconnect swaps the slot's immutable arms bundle without rebuilding the body.
//
// The type is also the registry.Constructor return shape.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Factory ActorFactory
}
