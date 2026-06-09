package platform

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// Gateway is the client/SDK ingress (v2): it receives independent substrate
// interfaces + the assembly-root's writer and PushHub, NOT a *channelhost.ChannelHome.
// It exposes pure Go methods; HTTP/WS transport is the app layer's concern.
//
// Gateway deliberately knows nothing about sender kind, TTL, or other product
// decisions -- those belong in the app layer. Its public surface is:
// SendMessage, ListMessages, ListActors, MaxSeq, ChannelID.
type Gateway struct {
	writer    harness.Writer         // postCommitWriter from assembly root
	hub       *PushHub               // client subscription signal
	channelID channel.ID
	query     storespec.MessageQuery // read path
	registry  storespec.Registry     // actor list
}

// SendMessage writes a client envelope through the harness pipeline.
func (g *Gateway) SendMessage(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return g.writer.Write(ctx, env)
}

// ListMessages returns committed messages after the given sequence number.
func (g *Gateway) ListMessages(ctx context.Context, after int64, limit int) ([]storespec.StoredRow, error) {
	return g.query.ReadAfterSeq(ctx, after, limit)
}

// ListActors returns all active actors in the channel.
func (g *Gateway) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return g.registry.ListActive(ctx)
}

// MaxSeq returns the highest committed sequence number.
func (g *Gateway) MaxSeq(ctx context.Context) (int64, error) {
	return g.query.MaxSeq(ctx)
}

// ChannelID returns the channel this gateway is bound to.
func (g *Gateway) ChannelID() channel.ID {
	return g.channelID
}
