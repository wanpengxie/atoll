package actorrt

import (
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Incarnation is the opaque identity of one exact Unit. Pointer identity of
// unit distinguishes a predecessor from its same-ActorID successor. It is
// process-local, comparable, and never serialized into collaboration truth.
type Incarnation struct {
	id   actor.ActorID
	unit *Unit
}

// ID exposes the ActorID label carried by this exact Unit. The Unit pointer
// remains opaque so only the execution owner can compare incarnations.
func (i Incarnation) ID() actor.ActorID { return i.id }

// UnitStat is the substrate-owned, read-only observation of one Unit.
type UnitStat struct {
	StartedAt time.Time
	Kind      actor.Kind
}
