package actorbase

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Msg is the substrate's in-hand projection of one delivered envelope — pure,
// immutable data with zero-effect methods (spec §1.2: "Msg=纯不可变数据零效果
// 方法,泄漏 msg=泄漏只读数据;全系统唯一有效果的把手=sys"). It carries the
// content fields a Proc body reads to decide what to do, projected 1:1 off
// message.Envelope, plus the one thing an envelope itself does not carry: the
// ctx this particular delivery is scoped to.
//
// Ctx is unexported and reached only through the Ctx() accessor (mirrors
// http.Request.Context()) — a request-scoped struct is the one place Go
// tolerates ctx-as-a-field, and keeping it unexported stops a caller from
// constructing a Msg with a stray ctx that did not come from the engine's own
// ledger. For a kind=request delivery, Ctx() is derived from the serve
// ledger's entry (deadline + cancel, §1.5); for event/timer/no-one-waiting
// deliveries (no closure obligation), Ctx() IS sys.Life() — one process-life
// ctx, one name, no second "background" ctx anywhere in this package.
type Msg struct {
	ID            message.ID
	TS            int64
	ChannelID     channel.ID
	Sender        message.Sender
	Kind          message.Kind
	Type          string
	Payload       json.RawMessage
	ParentID      message.ID
	CorrelationID message.ID
	Visibility    message.Visibility
	Audience      message.Audience
	ExpiresAt     *int64

	ctx context.Context
}

// Ctx returns the ctx this delivery is scoped to. See the Sys godoc's
// provenance rule for which ctx a Proc body should thread onward from here.
func (m Msg) Ctx() context.Context { return m.ctx }

// NewMsg projects env into a Msg bound to ctx — the ONE constructor (the
// engine's Recv path and any test fixture both go through it; there is no
// second, partial way to build a Msg). env is read, never retained: Msg is a
// value copy of env's content fields, so mutating the envelope after
// projection cannot reach back into an already-delivered Msg.
func NewMsg(ctx context.Context, env message.Envelope) Msg {
	return Msg{
		ID:            env.ID,
		TS:            env.TS,
		ChannelID:     env.ChannelID,
		Sender:        env.Sender,
		Kind:          env.Kind,
		Type:          env.Type,
		Payload:       env.Payload,
		ParentID:      env.ParentID,
		CorrelationID: env.CorrelationID,
		Visibility:    env.Visibility,
		Audience:      env.Audience,
		ExpiresAt:     env.ExpiresAt,
		ctx:           ctx,
	}
}
