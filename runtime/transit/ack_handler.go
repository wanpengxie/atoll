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
	outbox  OutboxReader
	cursors *CursorTracker
}

// NewAckHandler builds an AckHandler bound to one channel's outbox.
func NewAckHandler(outbox OutboxReader, cursors *CursorTracker) (*AckHandler, error) {
	if outbox == nil {
		return nil, errors.New("transit: NewAckHandler outbox nil")
	}
	if cursors == nil {
		return nil, errors.New("transit: NewAckHandler cursors nil")
	}
	return &AckHandler{outbox: outbox, cursors: cursors}, nil
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
