package platform

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// SpawnForTesting is a black-box fixture seam. It still uses production Admit
// and therefore cannot choose an actor id or reactivate a removed identity.
func SpawnForTesting(h *Home, kind actor.Kind, principal string, def ActorFactory) (actor.ActorID, error) {
	id, err := h.Admit(context.Background(), kind, principal)
	if err != nil {
		return "", err
	}
	_, built, err := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return build(h.buildCaps(id, kind, inc), h.hooks(), def)
	})
	if err != nil {
		return "", err
	}
	if !built {
		return "", fmt.Errorf("platform: testing spawn %q already occupied", id)
	}
	return id, nil
}
