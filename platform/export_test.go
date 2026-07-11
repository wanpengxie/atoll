package platform

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// AdmitExactForTest seeds historical fixed-id fixtures. Production admission
// cannot accept caller-supplied ids and always goes through Home.Admit.
func AdmitExactForTest(h *Home, id actor.ActorID, kind actor.Kind) error {
	if rec, ok, err := h.cs.Registry.Lookup(context.Background(), id); err != nil {
		return err
	} else if ok && rec.IsActive() {
		return nil
	}
	return h.cs.Membership.Insert(context.Background(), storespec.Record{ID: id, Kind: kind, CreatedAt: h.nowMs()})
}

func HandleCancelUpstreamForTest(h *Home, id actor.ActorID, requestID message.ID) {
	h.handleCancelUpstream(id, requestID)
}

func SpawnForTest(h *Home, id actor.ActorID, kind actor.Kind, def ActorFactory) error {
	if err := AdmitExactForTest(h, id, kind); err != nil {
		return err
	}
	_, built, err := h.channel.Cells().SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return build(h.buildCaps(id, kind, inc), h.hooks(), def)
	})
	if err != nil {
		return err
	}
	if !built {
		return fmt.Errorf("test spawn %q already occupied", id)
	}
	return nil
}
