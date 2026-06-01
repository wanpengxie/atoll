package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// OutboxAcker is the subset of OutboxReader the multi-channel ack
// handler needs (just AckUpTo). Declared so the daemon can hand back
// a per-channel sink without exposing the full reader.
type OutboxAcker interface {
	AckUpTo(ctx context.Context, lastAckedSeq viewsync.Seq) error
	ResetAllPushed(ctx context.Context) error
}

// AckRouter resolves a channel id → the outbox sink that should GC
// rows for the ack. ok=false ⇒ channel is not owned by this daemon
// (or has been unloaded) — the caller treats it as a silently-dropped
// ack.
type AckRouter func(channel.ID) (OutboxAcker, bool)

// MultiAckHandler routes incoming viewsync.ack frames to the per-
// channel cursor + outbox sink. Used by the daemon's transit
// dispatcher when many channels share one daemonbus connection.
type MultiAckHandler struct {
	cursors        *CursorTracker
	router         AckRouter
	onStaleFencing AckRejectHandler
}

// NewAckHandlerForChannels builds a MultiAckHandler.
func NewAckHandlerForChannels(cursors *CursorTracker, router AckRouter) (*MultiAckHandler, error) {
	return NewAckHandlerForChannelsWithRejectHandler(cursors, router, nil)
}

// NewAckHandlerForChannelsWithRejectHandler builds a MultiAckHandler with a
// stale-fencing reject hook.
func NewAckHandlerForChannelsWithRejectHandler(cursors *CursorTracker, router AckRouter, onStaleFencing AckRejectHandler) (*MultiAckHandler, error) {
	if cursors == nil {
		return nil, errors.New("transit: NewAckHandlerForChannels cursors nil")
	}
	if router == nil {
		return nil, errors.New("transit: NewAckHandlerForChannels router nil")
	}
	return &MultiAckHandler{cursors: cursors, router: router, onStaleFencing: onStaleFencing}, nil
}

// Handle implements the ControlHandlers.OnViewsyncAck signature.
func (h *MultiAckHandler) Handle(ctx context.Context, ack viewsync.AckFrame) error {
	sink, ok := h.router(ack.ChannelID)
	if !ok {
		// Drop silently — the channel was unloaded after the server
		// emitted the ack. Returning nil keeps the dispatch loop
		// running.
		return nil
	}
	if !ack.Accepted {
		if ack.RejectReason == viewsync.RejectReasonMuxOwnerEpochStale ||
			ack.RejectReason == viewsync.RejectReasonViewsyncResyncBackpressure {
			if h.onStaleFencing != nil {
				if err := h.onStaleFencing(ctx, ack); err != nil {
					return fmt.Errorf("transit: multi ack reject %s: %w", ack.ChannelID, err)
				}
			}
			return nil
		}
		if err := sink.ResetAllPushed(ctx); err != nil {
			return fmt.Errorf("transit: multi ack reset pushed %s: %w", ack.ChannelID, err)
		}
		return nil
	}
	if ack.LastReceivedSeq <= 0 {
		return nil
	}
	if !h.cursors.AdvanceAcked(ack.ChannelID, ack.LastReceivedSeq) {
		return nil
	}
	if err := sink.AckUpTo(ctx, viewsync.Seq(ack.LastReceivedSeq)); err != nil {
		return fmt.Errorf("transit: multi ack GC %s: %w", ack.ChannelID, err)
	}
	return nil
}

// ResyncSource is the per-channel reader the multi-channel resync
// server delegates to. MessageRangeReader satisfies it.
type ResyncSource = MessageRangeReader

// ResyncRouter resolves a channel id → the resync source bound to
// that channel.
type ResyncRouter func(channel.ID) (ResyncSource, bool)

// MultiResyncServer routes ResyncRequest frames by channel_id.
type MultiResyncServer struct {
	router ResyncRouter
}

// NewMultiResyncServer builds a MultiResyncServer.
func NewMultiResyncServer(router ResyncRouter) (*MultiResyncServer, error) {
	if router == nil {
		return nil, errors.New("transit: NewMultiResyncServer router nil")
	}
	return &MultiResyncServer{router: router}, nil
}

// ServeResync implements the ControlHandlers.OnViewsyncResyncRequest
// signature.
func (s *MultiResyncServer) ServeResync(ctx context.Context, req viewsync.ResyncRequest) (viewsync.ResyncResponse, error) {
	src, ok := s.router(req.ChannelID)
	if !ok {
		return viewsync.ResyncResponse{}, fmt.Errorf("transit: resync channel %q unknown", req.ChannelID)
	}
	srv, err := NewResyncServer(src)
	if err != nil {
		return viewsync.ResyncResponse{}, err
	}
	return srv.ServeResync(ctx, req)
}
