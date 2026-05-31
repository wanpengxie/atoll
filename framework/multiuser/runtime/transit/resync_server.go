package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/channel"
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

// MaxResyncChunkSize bounds each daemon-side outbox read used to answer
// a resync request. Larger closed intervals are paged by ServeResync.
const MaxResyncChunkSize = 500

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
	var all []viewsync.ResyncMessage
	for start := req.SinceSeq; start <= req.UntilSeq; {
		chunkUntil := resyncChunkUntil(start, req.UntilSeq)
		msgs, err := s.reader.MessagesByRange(ctx, start, chunkUntil)
		if err != nil {
			return viewsync.ResyncResponse{}, fmt.Errorf("transit: resync read: %w", err)
		}
		all = append(all, msgs...)
		if chunkUntil == req.UntilSeq {
			break
		}
		start = chunkUntil + 1
	}
	return viewsync.ResyncResponse{
		ChannelID: req.ChannelID,
		SinceSeq:  req.SinceSeq,
		UntilSeq:  req.UntilSeq,
		Messages:  all,
	}, nil
}

func resyncChunkUntil(since, until viewsync.Seq) viewsync.Seq {
	maxUntil := since + viewsync.Seq(MaxResyncChunkSize) - 1
	if maxUntil < since || maxUntil > until {
		return until
	}
	return maxUntil
}
