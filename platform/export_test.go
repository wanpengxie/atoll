package platform

import (
	"context"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func AdmitForTest(h *Home, principal string, kind actor.Kind) (actor.ActorID, error) {
	return h.Admit(context.Background(), kind, principal)
}

func HandleCancelUpstreamForTest(h *Home, id actor.ActorID, requestID message.ID) {
	h.handleCancelUpstream(id, requestID)
}

func SpawnForTest(h *Home, fixtureID actor.ActorID, kind actor.Kind, def ActorFactory) (actor.ActorID, error) {
	id := fixtureID
	if rec, ok, err := h.cs.Registry.Lookup(context.Background(), fixtureID); err != nil {
		return "", err
	} else if !ok || !rec.IsActive() {
		id, err = AdmitForTest(h, strings.ReplaceAll(string(fixtureID), ":", "-"), kind)
		if err != nil {
			return "", err
		}
	}
	_, built, err := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return build(h.buildCaps(id, kind, inc), h.hooks(), def)
	})
	if err != nil {
		return "", err
	}
	if !built {
		return "", fmt.Errorf("test spawn %q already occupied", id)
	}
	return id, nil
}
