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

// ActorFactoryResolver resolves only an exact immutable construction input.
// There is deliberately no ActorID-only lookup: a body build must not re-read
// whichever Definition happens to be current after its claim was created.
type ActorFactoryResolver interface {
	LookupByClass(actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type compositionView struct {
	h        *Home
	resolver CompositionResolver
}

func (v *compositionView) LookupByClass(
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}
