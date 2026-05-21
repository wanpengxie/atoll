package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/server/daemonbus"
	"github.com/wanpengxie/ActOS/server/viewcache"
)

// busResyncer satisfies viewcache.Resyncer by sending a
// viewsync.resync_request frame over the active daemonbus connection
// for the channel and waiting for the matching response.
type busResyncer struct {
	bus       *daemonbus.Service
	viewcache *viewcache.Service
}

// RequestResync issues a closed-interval resync RPC to the daemon
// owning channelID, waits for the response, and returns the
// recovered messages.
func (b *busResyncer) RequestResync(ctx context.Context, channelID channel.ID, since, until viewsync.Seq) ([]viewsync.ResyncMessage, error) {
	conn, err := b.bus.ConnectionForChannel(ctx, string(channelID))
	if err != nil {
		return nil, err
	}
	req := viewsync.ResyncRequest{ChannelID: channelID, SinceSeq: since, UntilSeq: until}
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ackFrame, err := conn.SendAndAwait(timeoutCtx, kerneldaemonbus.FrameTypeViewsyncResyncRequest, req)
	if err != nil {
		return nil, err
	}
	if ackFrame.FrameKind != kerneldaemonbus.FrameTypeViewsyncResyncResponse {
		return nil, errors.New("gateway: resync response wrong frame_type")
	}
	var body viewsync.ResyncResponse
	if err := json.Unmarshal(ackFrame.Payload, &body); err != nil {
		return nil, err
	}
	return body.Messages, nil
}
