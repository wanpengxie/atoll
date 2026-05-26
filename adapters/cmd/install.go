package cmd

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
)

// ActorSeed is the actor_registry row composition root MUST insert
// before installing the cmd Module on a channel. Embedded binding —
// no DeviceTransit / no token issuance / no cloud secret state.
func ActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:      DefaultAdapterActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingEmbedded,
	}
}
