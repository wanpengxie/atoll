package peeractor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type peerSys struct {
	actorbase.Sys
	recv  []actorbase.Msg
	reply json.RawMessage
	fail  string
}

func (s *peerSys) Recv() (actorbase.Msg, error) {
	if len(s.recv) == 0 {
		return actorbase.Msg{}, errors.New("done")
	}
	msg := s.recv[0]
	s.recv = s.recv[1:]
	return msg, nil
}
func (s *peerSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.reply, _ = json.Marshal(value)
	return "reply", nil
}
func (s *peerSys) Fail(_ actorbase.Msg, code, _ string) (message.ID, error) {
	s.fail = code
	return "fail", nil
}

func TestPeeractorSealsOriginFromLedgerEnvelopeAndReturnsOnlyBody(t *testing.T) {
	payload := json.RawMessage(`{"origin":{"channel":"forged"},"value":7}`)
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request-a", ChannelID: "caller", Kind: message.KindRequest, Type: "work",
		Sender: message.Sender{ID: "alice"}, Payload: payload,
	})
	sys := &peerSys{recv: []actorbase.Msg{msg}}
	var got peerproto.Request
	err := serve(sys, Deps{
		Caller: "caller", Target: "target",
		Seam: func(_ context.Context, caller, target channel.ID, request peerproto.Request) (peerproto.Result, error) {
			if caller != "caller" || target != "target" {
				t.Fatalf("route caller=%q target=%q", caller, target)
			}
			got = request
			return peerproto.Result{Body: json.RawMessage(`{"ok":true}`)}, nil
		},
		Card: func(context.Context, channel.ID, channel.ID) (introspect.Describe, error) {
			return introspect.Describe{}, nil
		},
	})
	if err == nil || got.Origin.Channel != "caller" || got.Origin.Actor != "alice" || got.Origin.RequestID != "request-a" || got.Type != "work" || string(got.Payload) != string(payload) {
		t.Fatalf("origin/request=%+v serveErr=%v", got, err)
	}
	if string(sys.reply) != `{"ok":true}` || sys.fail != "" {
		t.Fatalf("reply=%s fail=%q", sys.reply, sys.fail)
	}
}

func TestPeeractorUnavailableAndRemoteFailureUseClosedCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		seam Seam
		want string
	}{
		{name: "unavailable", want: "channel_unavailable", seam: func(context.Context, channel.ID, channel.ID, peerproto.Request) (peerproto.Result, error) {
			return peerproto.Result{}, errors.New("offline")
		}},
		{name: "remote failure", want: "endpoint_not_found", seam: func(context.Context, channel.ID, channel.ID, peerproto.Request) (peerproto.Result, error) {
			return peerproto.Result{Fail: &peerproto.Failure{Code: "endpoint_not_found", Detail: "missing"}}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "request", ChannelID: "caller", Kind: message.KindRequest, Type: "work"})
			sys := &peerSys{recv: []actorbase.Msg{msg}}
			_ = serve(sys, Deps{Caller: "caller", Target: "target", Seam: tc.seam, Card: func(context.Context, channel.ID, channel.ID) (introspect.Describe, error) {
				return introspect.Describe{}, nil
			}})
			if sys.fail != tc.want {
				t.Fatalf("fail=%q want=%q", sys.fail, tc.want)
			}
		})
	}
}

func TestPeeractorDescribeUsesSharedCardWithoutPortCall(t *testing.T) {
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "describe", ChannelID: "caller", Kind: message.KindRequest, Type: introspect.QueryDescribe})
	sys := &peerSys{recv: []actorbase.Msg{msg}}
	called := false
	_ = serve(sys, Deps{
		Caller: "caller", Target: "target",
		Seam: func(context.Context, channel.ID, channel.ID, peerproto.Request) (peerproto.Result, error) {
			called = true
			return peerproto.Result{}, nil
		},
		Card: func(context.Context, channel.ID, channel.ID) (introspect.Describe, error) {
			return introspect.Describe{ActorID: "c0.target", Types: map[string]introspect.TypeMeta{"work": {}}}, nil
		},
	})
	if called || sys.fail != "" || len(sys.reply) == 0 {
		t.Fatalf("called=%v fail=%q reply=%s", called, sys.fail, sys.reply)
	}
}
