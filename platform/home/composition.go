package home

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
)

// CompositionResolver is the world-catalog half of actor construction. Home
// supplies the channel-local committed class/config.
type CompositionResolver interface {
	BuildClass(channelpkg.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

// ActorFactoryResolver is the one construction lookup consumed by the Server
// Host builder.
type ActorFactoryResolver interface {
	Lookup(actor.ActorID) (platform.ActorFactory, bool)
	LookupByClass(actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type compositionView struct {
	h        *Home
	resolver CompositionResolver
}

func (v *compositionView) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	row, ok, err := v.h.actors.Lookup(id)
	if err != nil || !ok {
		return platform.ActorFactory{}, false
	}
	return v.resolver.BuildClass(v.h.channelID, id, row.Class, row.Config)
}

func (v *compositionView) LookupByClass(
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}
