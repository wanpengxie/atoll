package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/viewsync"
)

// MessageRangeReader is the subset of runtime/store.ViewSyncOutbox the
// resync server needs. Declared as an interface so tests can swap in
// fakes.
type MessageRangeReader interface {
	ChannelID() channel.ID
	MessagesByRange(ctx context.Context, since, until viewsync.Seq) ([]viewsync.ResyncMessage, error)
}

// ResyncServer is the daemon-side implementation of L1 §8.5: server
// requests a closed interval [since, until], daemon returns messages.
type ResyncServer struct {
	reader MessageRangeReader
}

// NewResyncServer builds a ResyncServer bound to one channel's reader.
func NewResyncServer(reader MessageRangeReader) (*ResyncServer, error) {
	if reader == nil {
		return nil, errors.New("transit: NewResyncServer reader nil")
	}
	return &ResyncServer{reader: reader}, nil
}

// ServeResync implements viewsync.Resyncer (daemon side).
func (s *ResyncServer) ServeResync(ctx context.Context, req viewsync.ResyncRequest) (viewsync.ResyncResponse, error) {
	if req.ChannelID != s.reader.ChannelID() {
		return viewsync.ResyncResponse{}, fmt.Errorf("transit: resync channel %q != reader channel %q",
			req.ChannelID, s.reader.ChannelID())
	}
	if req.SinceSeq > req.UntilSeq {
		return viewsync.ResyncResponse{}, fmt.Errorf("transit: resync range invalid: since=%d > until=%d",
			req.SinceSeq, req.UntilSeq)
	}
	msgs, err := s.reader.MessagesByRange(ctx, req.SinceSeq, req.UntilSeq)
	if err != nil {
		return viewsync.ResyncResponse{}, fmt.Errorf("transit: resync read: %w", err)
	}
	return viewsync.ResyncResponse{
		ChannelID: req.ChannelID,
		SinceSeq:  req.SinceSeq,
		UntilSeq:  req.UntilSeq,
		Messages:  msgs,
	}, nil
}
