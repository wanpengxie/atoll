package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
)

const firedSweepBatch = 256

// sweepFired makes one level-triggered delivery attempt per fired row per
// reconcile pass. A failed/full enqueue leaves the row untouched; the next
// pass tries again. Ack is the only successful consumption transition.
func (h *Home) sweepFired(ctx context.Context) {
	if h.cs.FiredTimers == nil {
		return
	}
	page, err := h.cs.FiredTimers.ListFired(ctx, h.firedCursor, firedSweepBatch)
	if err != nil {
		h.logger.Error("timer fired scan", "channel", string(h.channelID), "err", err)
		return
	}
	for _, timer := range page.Rows {
		env, ok, err := h.cs.Requests.FindByID(ctx, message.ID("timer:"+string(timer.ID)))
		if err != nil || !ok {
			h.logger.Error("timer fired truth lookup", "timer", string(timer.ID), "found", ok, "err", err)
			continue
		}
		// Accept durable truth before attempting revival. With no carrier this
		// records wake debt; with a carrier it delivers exactly once this pass.
		// Revival is only an accelerator and may not suppress that transition.
		verdict, err := h.liveness.AcceptFiredDelivery(timer.AuthorID, env)
		if err != nil || verdict != transitionApplied {
			h.logger.Warn("timer fired redeliver", "timer", string(timer.ID), "err", err)
			continue
		}
		if err := (homeReviver{h: h}).EnsureLive(ctx, timer.AuthorID); err != nil {
			h.logger.Warn("timer fired revive", "timer", string(timer.ID), "author", string(timer.AuthorID), "err", err)
		}
		h.pokeReconcile()
	}
	if page.Done {
		h.firedCursor = runtime.FiredTimerCursor{}
	} else {
		h.firedCursor = page.Next
	}
}
