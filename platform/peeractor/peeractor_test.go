package peeractor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type peerSys struct {
	actorbase.Sys
	recv     []actorbase.Msg
	reply    json.RawMessage
	fail     string
	progress []string
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
func (s *peerSys) Progress(_ actorbase.Msg, status string, _ any) (message.ID, error) {
	s.progress = append(s.progress, status)
	return "progress", nil
}

func TestPeeractorSealsOriginFromLedgerEnvelopeAndReturnsOnlyBody(t *testing.T) {
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request-a", ChannelID: "caller", Kind: message.KindRequest, Type: "work",
		Sender: message.Sender{ID: "alice"}, Payload: json.RawMessage(`{"body":{"origin":{"channel":"forged"},"value":7}}`),
	})
	sys := &peerSys{recv: []actorbase.Msg{msg}}
	var got channel.Request
	err := serve(sys, Deps{
		Caller: "caller", Target: "target",
		Seam: func(_ context.Context, caller, target channel.ID, request channel.Request, _ func(channel.Progress)) (channel.Result, error) {
			if caller != "caller" || target != "target" {
				t.Fatalf("route caller=%q target=%q", caller, target)
			}
			got = request
			return channel.Result{Body: json.RawMessage(`{"ok":true}`)}, nil
		},
		Describe: func(context.Context, channel.ID, channel.ID, channel.Describe) (channel.Card, error) {
			return channel.Card{Words: map[string]json.RawMessage{}}, nil
		},
	})
	if err == nil || got.From.Channel != "caller" || got.From.Actor != "alice" || got.From.RequestID != "request-a" || got.Type != "work" || string(got.Payload) != `{"origin":{"channel":"forged"},"value":7}` {
		t.Fatalf("origin/request=%+v serveErr=%v", got, err)
	}
	if string(sys.reply) != `{"ok":true}` || sys.fail != "" {
		t.Fatalf("reply=%s fail=%q", sys.reply, sys.fail)
	}
}

func TestPeeractorUnavailableAndRemoteFailureUseClosedCodes(t *testing.T) {
	tests := []struct {
		name string
		seam Seam
		want string
	}{
		{name: "unavailable", want: "channel_unavailable", seam: func(context.Context, channel.ID, channel.ID, channel.Request, func(channel.Progress)) (channel.Result, error) {
			return channel.Result{}, errors.New("offline")
		}},
	}
	for _, code := range []string{"endpoint_not_found", "receiver_inactive", "bad_origin", "forbidden", "no_service_agent", "receiver_unavailable", "unanswered_timeout", "channel_unavailable"} {
		code := code
		tests = append(tests, struct {
			name string
			seam Seam
			want string
		}{name: "remote " + code, want: code, seam: func(context.Context, channel.ID, channel.ID, channel.Request, func(channel.Progress)) (channel.Result, error) {
			return channel.Result{Fail: &channel.Failure{Stage: channel.StageGate, Code: code, Detail: "remote failure"}}, nil
		}})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "request", ChannelID: "caller", Kind: message.KindRequest, Type: "work", Payload: json.RawMessage(`{"body":null}`)})
			sys := &peerSys{recv: []actorbase.Msg{msg}}
			_ = serve(sys, Deps{Caller: "caller", Target: "target", Seam: tc.seam, Describe: func(context.Context, channel.ID, channel.ID, channel.Describe) (channel.Card, error) {
				return channel.Card{Words: map[string]json.RawMessage{}}, nil
			}})
			if sys.fail != tc.want {
				t.Fatalf("fail=%q want=%q", sys.fail, tc.want)
			}
		})
	}
}

func TestPeeractorRelaysProgressBeforeTerminal(t *testing.T) {
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request", ChannelID: "caller", Kind: message.KindRequest, Type: "agent.ask", Payload: json.RawMessage(`{"body":{"text":"hi"}}`),
	})
	sys := &peerSys{recv: []actorbase.Msg{msg}}
	_ = serve(sys, Deps{Caller: "caller", Target: "target", Seam: func(_ context.Context, _, _ channel.ID, request channel.Request, progress func(channel.Progress)) (channel.Result, error) {
		progress(channel.Progress{RequestID: request.From.RequestID, Seq: 1, Status: message.StatusProcessing, Body: json.RawMessage(`{"step":1}`)})
		return channel.Result{Body: json.RawMessage(`{"done":true}`)}, nil
	}, Describe: func(context.Context, channel.ID, channel.ID, channel.Describe) (channel.Card, error) {
		return channel.Card{Words: map[string]json.RawMessage{}}, nil
	}})
	if len(sys.progress) != 1 || sys.progress[0] != message.StatusProcessing || string(sys.reply) != `{"done":true}` {
		t.Fatalf("progress=%v reply=%s", sys.progress, sys.reply)
	}
}

func TestPeeractorDynamicManifestIsOneRemoteCardProjection(t *testing.T) {
	calls := 0
	def := Def(Deps{Caller: "caller", Target: "target", Seam: func(context.Context, channel.ID, channel.ID, channel.Request, func(channel.Progress)) (channel.Result, error) {
		return channel.Result{}, nil
	}, Describe: func(_ context.Context, caller, target channel.ID, frame channel.Describe) (channel.Card, error) {
		calls++
		if caller != "caller" || target != "target" || frame.From.Channel != "caller" {
			t.Fatalf("describe caller=%q target=%q frame=%+v", caller, target, frame)
		}
		spec, _ := json.Marshal(introspect.WordSpec{Description: "remote"})
		return channel.Card{Words: map[string]json.RawMessage{"work.run": spec}}, nil
	}})
	words, err := def.Manifest.Dynamic(context.Background())
	if err != nil || calls != 1 || len(words) != 1 || words["work.run"].Description != "remote" {
		t.Fatalf("words=%+v calls=%d err=%v", words, calls, err)
	}
}
