package lagoon

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

type callerStub struct {
	err     error
	word    Word
	payload message.Envelope
}

func (c *callerStub) CallRegistrar(_ context.Context, word Word, payload any) (json.RawMessage, error) {
	c.word = word
	raw, _ := json.Marshal(payload)
	_ = json.Unmarshal(raw, &c.payload)
	if c.err != nil {
		return nil, c.err
	}
	reply, _ := json.Marshal(Reply{Word: word, Value: json.RawMessage(`{"ok":true}`), Source: SourceRef{ChannelID: c.payload.ChannelID, RequestID: string(c.payload.ID)}})
	return reply, nil
}

func TestSubmitCarriesSourceFactsForReceiverAttribution(t *testing.T) {
	caller := &callerStub{}
	s := NewSubmitter(caller)
	reply, err := s.Submit(context.Background(), SubmitIn{Source: "home", Sender: "human:alice", RequestID: "request-1", Word: WordChannelCreate, Payload: ChannelCreate{Name: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	wantSource := SourceRef{ChannelID: "home", RequestID: "request-1"}
	if caller.payload.ChannelID != "home" || caller.payload.ID != "request-1" || caller.payload.Sender.ID != "human:alice" || reply.Source != wantSource {
		t.Fatalf("forward=%+v reply=%+v", caller.payload, reply)
	}
}

func TestSubmitDeadlineReturnsUnknownWithSourceAnchor(t *testing.T) {
	caller := &callerStub{err: context.DeadlineExceeded}
	s := NewSubmitter(caller)
	_, err := s.Submit(context.Background(), SubmitIn{Source: "home", Sender: "human:alice", RequestID: "request-2", Word: WordChannelCreate, Payload: ChannelCreate{Name: "x"}})
	var lagoonErr *Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != CodeResultUnknown || lagoonErr.Detail != "home:request-2" {
		t.Fatalf("error=%v", err)
	}
}

func TestApplicationEntranceAcceptsOnlyRegister(t *testing.T) {
	s := NewSubmitter(&callerStub{})
	if _, err := s.SubmitApplication(context.Background(), WordDeviceMint, DeviceMint{Name: "x"}); err == nil {
		t.Fatal("application entrance accepted an authenticated word")
	}
}
