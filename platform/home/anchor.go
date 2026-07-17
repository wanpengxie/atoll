package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// redeliverOpenRequests performs exactly one truth-derived anchor scan after a
// carrier handoff is published. It has no retry loop and stores no envelope in
// L: a full carrier leaves the request open for the next handoff (or caller
// deadline), while actorbase's serve admission makes a same-id duplicate to an
// already-live incarnation a no-op.
func (h *Home) redeliverOpenRequests(ctx context.Context, id actor.ActorID) {
	rows, err := h.cs.Query.OpenRequestsForActor(ctx, id)
	if err != nil {
		h.logger.Warn("platform.anchor.scan_failed", "actor", id, "err", err)
		return
	}
	for _, row := range rows {
		env := row.Envelope
		verdict, err := h.liveness.AcceptDelivery(id, &env)
		if err != nil || verdict != transitionApplied {
			h.logger.Warn("platform.anchor.redeliver_failed", "actor", id, "message", env.ID, "err", err)
		}
	}
}
