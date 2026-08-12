package spacetool

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type spaceToolSysStub struct {
	actorbase.Sys
	code   string
	detail string
}

func (s *spaceToolSysStub) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.code, s.detail = code, detail
	return "failed", nil
}

func TestUnclassifiedFailureUsesClosedResultUnknownCode(t *testing.T) {
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request", ChannelID: channel.ID("ordinary"), Type: string(lagoon.WordChannelCreate),
		Kind: message.KindRequest, Sender: message.Sender{Kind: actor.KindHuman, ID: "human:root"},
	})
	sys := &spaceToolSysStub{}
	handle(sys, nil, msg)
	if sys.code != string(lagoon.CodeResultUnknown) || sys.detail != "space-tool unavailable" {
		t.Fatalf("failure=(%q,%q), want result_unknown fallback", sys.code, sys.detail)
	}
}
