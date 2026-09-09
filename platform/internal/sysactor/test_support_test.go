package sysactor

import (
	"context"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type fakeSys struct {
	actorbase.Sys
	replies []replyRec
}

type replyRec struct {
	msg actorbase.Msg
	v   any
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyRec{msg: msg, v: v})
	return msg.ID, nil
}

func requestMsg(id message.ID, typ string, payload []byte) actorbase.Msg {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: id, ChannelID: "ch", Kind: message.KindRequest, Type: typ,
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "agent:caller:1"},
		Audience: message.Audience{actor.SystemActorID}, Payload: payload,
	})
}
