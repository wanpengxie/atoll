package peeractor

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Seam func(context.Context, channel.ID, channel.ID, peerproto.Request) (peerproto.Result, error)
type Card func(context.Context, channel.ID, channel.ID) (introspect.Describe, error)

type Deps struct {
	Caller channel.ID
	Target channel.ID
	Seam   Seam
	Card   Card
}

func ValidateConfig(raw json.RawMessage) (channel.ID, error) {
	var cfg struct {
		Channel channel.ID `json:"channel"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Channel == "" {
		return "", errors.New("peeractor: config.channel required")
	}
	return cfg.Channel, nil
}

func Def(deps Deps) actorbase.Def {
	return actorbase.Def{Doc: "reference to channel " + string(deps.Target), New: func() (actorbase.Proc, error) {
		if deps.Caller == "" || deps.Target == "" || deps.Seam == nil || deps.Card == nil {
			return nil, errors.New("peeractor: incomplete dependencies")
		}
		return func(sys actorbase.Sys) error { return serve(sys, deps) }, nil
	}}
}

func serve(sys actorbase.Sys, deps Deps) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		if msg.Type == introspect.QueryDescribe {
			card, err := deps.Card(msg.Ctx(), deps.Target, deps.Caller)
			if err != nil {
				_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
			} else {
				_, _ = sys.Reply(msg, card)
			}
			continue
		}
		request := peerproto.Request{
			Origin: peerproto.Origin{Channel: msg.ChannelID, Actor: msg.Sender.ID, RequestID: msg.ID},
			Type:   msg.Type, Payload: append(json.RawMessage(nil), msg.Payload...),
		}
		result, err := deps.Seam(msg.Ctx(), deps.Caller, deps.Target, request)
		if err != nil {
			_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
			continue
		}
		if result.Fail != nil {
			_, _ = sys.Fail(msg, result.Fail.Code, result.Fail.Detail)
			continue
		}
		body := result.Body
		if len(body) == 0 {
			body = json.RawMessage(`{}`)
		}
		_, _ = sys.Reply(msg, body)
	}
}
