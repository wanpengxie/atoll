package app

import (
	"context"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// gateway is the app layer's client/SDK ingress, assembled from a Home's narrow
// capability set (Gate + View). platform delivers the钥匙 (write门, observation);
// the app composes the ingress shape it wants around them. Gateway deliberately
// knows nothing about sender kind, TTL, or envelope shape decisions beyond
// passing them through — those product decisions live in the handlers above it.
type gateway struct {
	channelID channel.ID
	gate      harness.Writer
	view      platform.View
}

// homeGateway builds an app-layer gateway over a channel home's capabilities.
func homeGateway(chID channel.ID, home *platform.Home) gateway {
	return gateway{channelID: chID, gate: home.Gate(), view: home.View()}
}

// SendMessage writes a client envelope through the home's commit write门.
func (g gateway) SendMessage(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return g.gate.Write(ctx, env)
}

// ListMessages returns committed messages after the given sequence number.
func (g gateway) ListMessages(ctx context.Context, after int64, limit int) ([]storespec.StoredRow, error) {
	return g.view.ReadAfterSeq(ctx, after, limit)
}

// ListActors returns all active actors in the channel.
func (g gateway) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return g.view.ListActors(ctx)
}

// MaxSeq returns the highest committed sequence number.
func (g gateway) MaxSeq(ctx context.Context) (int64, error) {
	return g.view.MaxSeq(ctx)
}

// ChannelID returns the channel this gateway is bound to.
func (g gateway) ChannelID() channel.ID { return g.channelID }
