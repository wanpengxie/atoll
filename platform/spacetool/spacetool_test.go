package spacetool

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type callerStub struct {
	word lagoon.Word
	env  message.Envelope
	raw  json.RawMessage
	err  error
}

func (c *callerStub) CallRegistrar(_ context.Context, word lagoon.Word, payload any) (json.RawMessage, error) {
	c.word = word
	c.env = payload.(message.Envelope)
	return c.raw, c.err
}

type spaceToolSysStub struct {
	actorbase.Sys
	code   string
	detail string
	value  any
}

func (s *spaceToolSysStub) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.code, s.detail = code, detail
	return "failed", nil
}

func (s *spaceToolSysStub) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.value = value
	return "reply", nil
}

func request(typ string, payload json.RawMessage) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request", ChannelID: channel.ID("ordinary"), Type: typ, Payload: payload,
		Kind: message.KindRequest, Sender: message.Sender{Kind: actor.KindHuman, ID: "human:root"},
	})
}

func reply(value json.RawMessage) json.RawMessage {
	raw, _ := json.Marshal(lagoon.Reply{Word: lagoon.WordChannelCreate, Value: value})
	return raw
}

func TestForwardNormalizesEmptyPayloadAndPreservesUnknownFields(t *testing.T) {
	for name, payload := range map[string]json.RawMessage{
		"empty":          nil,
		"unknown fields": json.RawMessage(`{"name":"x","future":{"enabled":true}}`),
	} {
		t.Run(name, func(t *testing.T) {
			caller := &callerStub{raw: reply(json.RawMessage(`{"ok":true}`))}
			sys := &spaceToolSysStub{}
			handle(sys, caller, request("future.registrar.word", payload))
			want := payload
			if len(want) == 0 {
				want = json.RawMessage(`{}`)
			}
			if caller.word != "future.registrar.word" || string(caller.env.Payload) != string(want) {
				t.Fatalf("forward=(%q,%s), want word and payload unchanged", caller.word, caller.env.Payload)
			}
			got, ok := sys.value.(json.RawMessage)
			if !ok || string(got) != `{"ok":true}` {
				t.Fatalf("reply payload=(%T,%s), want raw value", sys.value, got)
			}
		})
	}
}

func TestMalformedPayloadIsRejectedBeforeForward(t *testing.T) {
	caller := &callerStub{}
	sys := &spaceToolSysStub{}
	handle(sys, caller, request(string(lagoon.WordChannelCreate), json.RawMessage(`{} {}`)))
	if sys.code != string(lagoon.CodeInvalidArgs) || caller.env.ID != "" {
		t.Fatalf("failure=%q forward=%+v", sys.code, caller.env)
	}
}

func TestRegistrarFailureIsRebuiltOnSourceLeg(t *testing.T) {
	caller := &callerStub{err: &lagoon.Error{Code: lagoon.CodeNotFound, Detail: "from registrar"}}
	sys := &spaceToolSysStub{}
	handle(sys, caller, request("unknown.word", json.RawMessage(`{"future":true}`)))
	if sys.code != string(lagoon.CodeNotFound) || sys.detail != "from registrar" {
		t.Fatalf("failure=(%q,%q)", sys.code, sys.detail)
	}
}

func TestReplyValueGateRejectsMissingNullAndMalformed(t *testing.T) {
	for name, value := range map[string]json.RawMessage{
		"missing": nil, "null": json.RawMessage(`null`), "malformed": json.RawMessage(`{"x":`),
	} {
		t.Run(name, func(t *testing.T) {
			caller := &callerStub{raw: reply(value)}
			sys := &spaceToolSysStub{}
			handle(sys, caller, request(string(lagoon.WordChannelList), nil))
			if sys.code != string(lagoon.CodeResultUnknown) || sys.value != nil {
				t.Fatalf("gate failure=(%q,%q) value=%v", sys.code, sys.detail, sys.value)
			}
		})
	}
}

func TestUnclassifiedFailureUsesClosedResultUnknownCode(t *testing.T) {
	sys := &spaceToolSysStub{}
	handle(sys, &callerStub{err: errors.New("corridor unavailable")}, request(string(lagoon.WordChannelCreate), nil))
	if sys.code != string(lagoon.CodeResultUnknown) || sys.detail != "corridor unavailable" {
		t.Fatalf("failure=(%q,%q)", sys.code, sys.detail)
	}
}
