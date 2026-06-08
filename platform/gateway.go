package platform

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// Gateway is the client/SDK ingress (v2): it receives independent substrate
// interfaces + the assembly-root's writer and PushHub, NOT a *channelhost.ChannelHome.
// It exposes pure Go methods; HTTP/WS transport is the app layer's concern.
type Gateway struct {
	writer    harness.Writer         // postCommitWriter from assembly root
	hub       *PushHub               // client subscription signal
	channelID channel.ID
	query     storespec.MessageQuery // read path
	registry  storespec.Registry     // actor list
}

const clientRequestTTLMs int64 = 30_000

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

// NewClientEnvelope is a helper that builds a message.Envelope from typical
// client-provided fields, filling in defaults (ID, Kind, TTL). The caller is
// responsible for passing the result to SendMessage with an appropriate context.
func (g *Gateway) NewClientEnvelope(
	senderID actor.ActorID,
	msgID string,
	msgType string,
	kind message.Kind,
	payload []byte,
	audience []actor.ActorID,
) *message.Envelope {
	now := time.Now().UnixMilli()
	exp := now + clientRequestTTLMs

	envID := message.ID(msgID)
	if envID == "" {
		envID = message.ID(uuid.NewString())
	}
	if kind == "" {
		kind = message.KindRequest
	}

	aud := make(message.Audience, 0, len(audience))
	aud = append(aud, audience...)

	return &message.Envelope{
		ID:        envID,
		TS:        now,
		ChannelID: g.channelID,
		Kind:      kind,
		Type:      msgType,
		Sender:    message.Sender{Kind: actor.KindHuman, ID: senderID},
		Audience:  aud,
		Payload:   payload,
		ExpiresAt: &exp,
	}
}

// CallerContext returns a harness.CallerContext for the given actor, suitable
// for use with harness.CtxWithCaller before calling SendMessage.
func (g *Gateway) CallerContext(actorID actor.ActorID) harness.CallerContext {
	return harness.CallerContext{
		ActorID:   actorID,
		ChannelID: g.channelID,
	}
}
