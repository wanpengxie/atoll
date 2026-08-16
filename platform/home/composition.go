package home

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
)

// CompositionResolver is the world-catalog half of actor construction. Home
// supplies the channel-local committed class/config.
type CompositionResolver interface {
	BuildClass(channelpkg.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type ServiceCompositionResolver interface {
	BuildServiceClass(channelpkg.ID, actor.ActorID, *svcactor.Port, svcactor.Audit) (platform.ActorFactory, bool)
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
	if class == svcactor.Class {
		if resolver, ok := v.resolver.(ServiceCompositionResolver); ok {
			audit := func(ctx context.Context, payload map[string]any) error {
				return v.h.emitSystemEvent(ctx, "svcactor.inbound", payload)
			}
			return resolver.BuildServiceClass(v.h.channelID, id, v.h.servicePort, audit)
		}
		return platform.ActorFactory{}, false
	}
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}
