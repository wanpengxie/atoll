package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

// AckHandler advances cursors + GCs outbox when viewsync.ack frames
// arrive (L1 §8.6).
//
// One AckHandler per channel; the central transit dispatcher routes
// incoming ack.ChannelID to the right handler.
type AckHandler struct {
	outbox         OutboxReader
	cursors        *CursorTracker
	onStaleFencing AckRejectHandler
}

type AckRejectHandler func(context.Context, viewsync.AckFrame) error

// NewAckHandler builds an AckHandler bound to one channel's outbox.
func NewAckHandler(outbox OutboxReader, cursors *CursorTracker) (*AckHandler, error) {
	return NewAckHandlerWithRejectHandler(outbox, cursors, nil)
}

// NewAckHandlerWithRejectHandler builds an AckHandler with a stale-fencing
// reject hook. The hook is where callers pause the channel pump and trigger
// placement reconciliation.
func NewAckHandlerWithRejectHandler(outbox OutboxReader, cursors *CursorTracker, onStaleFencing AckRejectHandler) (*AckHandler, error) {
	if outbox == nil {
		return nil, errors.New("transit: NewAckHandler outbox nil")
	}
	if cursors == nil {
		return nil, errors.New("transit: NewAckHandler cursors nil")
	}
	return &AckHandler{outbox: outbox, cursors: cursors, onStaleFencing: onStaleFencing}, nil
}

// Handle applies an incoming viewsync.ack: advance last_acked_seq in
// memory + delete outbox rows with seq <= LastReceivedSeq.
//
// Returns ok=true when the ack actually advanced state; ok=false (no
// error) when the ack was for a seq we already considered acked.
func (h *AckHandler) Handle(ctx context.Context, ack viewsync.AckFrame) (bool, error) {
	if ack.ChannelID != h.outbox.ChannelID() {
		return false, fmt.Errorf("transit: ack channel %q != outbox channel %q",
			ack.ChannelID, h.outbox.ChannelID())
	}
	if !ack.Accepted {
		if ack.RejectReason == viewsync.RejectReasonMuxOwnerEpochStale {
			if h.onStaleFencing != nil {
				if err := h.onStaleFencing(ctx, ack); err != nil {
					return false, fmt.Errorf("transit: ack stale fencing handler: %w", err)
				}
			}
			return false, nil
		}
		if resetter, ok := h.outbox.(interface{ ResetAllPushed(context.Context) error }); ok {
			if err := resetter.ResetAllPushed(ctx); err != nil {
				return false, fmt.Errorf("transit: ack reset pushed: %w", err)
			}
		}
		return false, nil
	}
	if ack.LastReceivedSeq <= 0 {
		return false, nil
	}
	if !h.cursors.AdvanceAcked(ack.ChannelID, ack.LastReceivedSeq) {
		// Already past this point — idempotent ack.
		return false, nil
	}
	if err := h.outbox.AckUpTo(ctx, viewsync.Seq(ack.LastReceivedSeq)); err != nil {
		return false, fmt.Errorf("transit: ack GC: %w", err)
	}
	return true, nil
}

// EnsureChannel registers a channel cursor entry (zero values) so calls
// to Get always return ok=true after registration.
func (h *AckHandler) EnsureChannel(channelID channel.ID) {
	h.cursors.AdvanceAcked(channelID, 0) // no-op if already past
}
