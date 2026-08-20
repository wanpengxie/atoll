package home

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/svcactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// CompositionResolver is the world-catalog half of actor construction. Home
// supplies the channel-local committed class/config.
type CompositionResolver interface {
	BuildClass(channelpkg.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type ServiceCompositionResolver interface {
	BuildServiceClass(channelpkg.ID, actor.ActorID, *svcactor.Port, svcactor.Audit, svcactor.Members) (platform.ActorFactory, bool)
}

type PeerCompositionResolver interface {
	Peer(context.Context, channelpkg.ID, channelpkg.ID, channelpkg.Request, func(channelpkg.Progress)) (channelpkg.Result, error)
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
			audit := func(ctx context.Context, cause message.Cause, payload map[string]any) error {
				return v.h.emitSystemEvent(ctx, cause, message.TypeSystemChannelInbound, payload)
			}
			members := svcactor.Members{
				IsActive: v.h.actors.IsActive,
				ActorFacts: func(ctx context.Context, id actor.ActorID) (svcactor.MemberFacts, bool, error) {
					facts, active, err := v.h.actors.ActorFacts(ctx, id)
					return svcactor.MemberFacts{Kind: facts.Kind}, active, err
				},
				FirstActiveAgent: func(context.Context) (actor.ActorID, bool, error) {
					rows, err := v.h.actors.ActiveIdentities()
					if err != nil {
						return "", false, err
					}
					var first actor.ActorID
					for _, row := range rows {
						if row.Kind == actor.KindAgent && (first == "" || row.ID < first) {
							first = row.ID
						}
					}
					return first, first != "", nil
				},
			}
			return resolver.BuildServiceClass(v.h.channelID, id, v.h.servicePort, audit, members)
		}
		return platform.ActorFactory{}, false
	}
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}
