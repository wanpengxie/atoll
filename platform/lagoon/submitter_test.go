package lagoon

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type callerStub struct {
	err     error
	word    Word
	payload forwardedRequest
}

func (c *callerStub) CallRegistrar(_ context.Context, word Word, payload any) (json.RawMessage, error) {
	c.word = word
	raw, _ := json.Marshal(payload)
	_ = json.Unmarshal(raw, &c.payload)
	if c.err != nil {
		return nil, c.err
	}
	reply, _ := json.Marshal(Reply{Word: word, Value: map[string]bool{"ok": true}, Source: c.payload.Source})
	return reply, nil
}

type sourceFactsStub struct{ facts channelspec.ActorFacts }

func (s sourceFactsStub) ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return s.facts, true, nil
}

func TestSubmitSealsSourceAndAttribution(t *testing.T) {
	caller := &callerStub{}
	s := NewSubmitter(caller, sourceFactsStub{facts: channelspec.ActorFacts{Principal: "alice", Kind: actor.KindHuman, Active: true}}, nil)
	reply, err := s.Submit(context.Background(), SubmitIn{Source: "home", Sender: "human:alice", RequestID: "request-1", Word: WordChannelCreate, Payload: ChannelCreate{Name: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if caller.payload.Initiator != "alice" || caller.payload.Source != (SourceRef{ChannelID: "home", RequestID: "request-1"}) || reply.Source != caller.payload.Source {
		t.Fatalf("forward=%+v reply=%+v", caller.payload, reply)
	}
}

func TestSubmitDeadlineReturnsUnknownWithSourceAnchor(t *testing.T) {
	caller := &callerStub{err: context.DeadlineExceeded}
	s := NewSubmitter(caller, sourceFactsStub{facts: channelspec.ActorFacts{Principal: "alice", Active: true}}, nil)
	_, err := s.Submit(context.Background(), SubmitIn{Source: "home", Sender: "human:alice", RequestID: "request-2", Word: WordChannelCreate, Payload: ChannelCreate{Name: "x"}})
	var lagoonErr *Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != CodeResultUnknown || lagoonErr.Detail != "home:request-2" {
		t.Fatalf("error=%v", err)
	}
}

func TestApplicationEntranceAcceptsOnlyRegister(t *testing.T) {
	s := NewSubmitter(&callerStub{}, nil, nil)
	if _, err := s.SubmitApplication(context.Background(), WordDeviceMint, DeviceMint{Name: "x"}); err == nil {
		t.Fatal("application entrance accepted an authenticated word")
	}
}
