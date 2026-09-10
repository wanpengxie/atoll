package channelmember

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"testing"
)

type memberStub struct {
	active bool
	decl   string
	actor  actor.ActorID
}

func (m memberStub) MemberOfDeclaration(string) (actor.ActorID, error) { return "tool:target:1", nil }

func (m memberStub) ActorFacts(_ context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	if m.actor != "" && id != m.actor {
		return channelspec.ActorFacts{}, false, nil
	}
	return channelspec.ActorFacts{Active: m.active, SourceDeclID: m.decl}, true, nil
}

type testSys struct {
	actorbase.Sys
	msgs     []actorbase.Msg
	failure  string
	events   []behavior.EventSpec
	posts    []behavior.RequestSpec
	caller   harness.Caller
	callSpec behavior.RequestSpec
}

func (s *testSys) Self() actor.ActorID { return "tool:handle:1" }
func (s *testSys) Recv() (actorbase.Msg, error) {
	if len(s.msgs) == 0 {
		return actorbase.Msg{}, errors.New("done")
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	return m, nil
}
func (s *testSys) Fail(_ actorbase.Msg, code, detail string, _ ...map[string]any) (message.ID, error) {
	s.failure = code
	return "failure", nil
}
func (s *testSys) Reply(actorbase.Msg, any) (message.ID, error) { return "reply", nil }
func (s *testSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.events = append(s.events, spec)
	return "event", nil
}
func (s *testSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	s.posts = append(s.posts, spec)
	return "post", nil
}
func (s *testSys) CallSpecFor(c harness.Caller, spec behavior.RequestSpec) (actorbase.Pending, error) {
	s.caller = c
	s.callSpec = spec
	return nil, errors.New("captured")
}

func TestHandleProcFailsWhenPortOccupied(t *testing.T) {
	hub := NewHub()
	pair := Pair{Host: "host", Body: "body"}
	release, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) { return Response{Payload: []byte("original")}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	proc, err := HandleDef(hub, pair.Body, HandleConfig{Host: pair.Host}, memberStub{}).New()
	if err != nil {
		t.Fatal(err)
	}
	if err := proc(&testSys{}); !errors.Is(err, ErrPortBusy) {
		t.Fatalf("proc exit=%v want port_busy", err)
	}
	if got, err := hub.Deliver(context.Background(), pair, Request{}); err != nil || string(got.Payload) != "original" {
		t.Fatalf("original binding lost: %+v %v", got, err)
	}
}

func TestHandleChecksAuthenticatedSenderBeforeDrivers(t *testing.T) {
	for _, tc := range []struct {
		name, payload, sender string
		members               memberStub
		drivers               []string
		want                  string
	}{
		{"caller metadata does not override sender", `{"body":{"type":"echo","payload":{},"audience":["tool:x:1"]},"_context":{"caller":{"channel":"foreign","actor":"tool:other:1"}}}`, "tool:driver:1", memberStub{active: true, decl: "driver", actor: "tool:driver:1"}, []string{"driver"}, "channel_unavailable"},
		{"forged allowed caller does not admit sender", `{"body":{"type":"echo","payload":{},"audience":["tool:x:1"]},"_context":{"caller":{"channel":"body","actor":"tool:driver:1"}}}`, "tool:intruder:1", memberStub{active: true, decl: "driver", actor: "tool:driver:1"}, []string{"driver"}, "forbidden"},
		{"inactive", `{"_context":{},"body":{"type":"echo","payload":{},"audience":["tool:x:1"]}}`, "tool:driver:1", memberStub{active: false, decl: "driver"}, nil, "forbidden"},
		{"wrong declaration", `{"_context":{},"body":{"type":"echo","payload":{},"audience":["tool:x:1"]}}`, "tool:driver:1", memberStub{active: true, decl: "other"}, []string{"driver"}, "forbidden"},
		{"allowed but disconnected", `{"_context":{},"body":{"type":"echo","payload":{},"audience":["tool:x:1"]}}`, "tool:driver:1", memberStub{active: true, decl: "driver"}, []string{"driver"}, "channel_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, body, err := harness.UnwrapPayload(json.RawMessage(tc.payload))
			if err != nil {
				t.Fatal(err)
			}
			m := actorbase.NewBodyMsgContext(actorbase.OriginMailbox, context.Background(), app, message.Envelope{ID: "request", ChannelID: "body", Sender: message.Sender{Kind: actor.KindTool, ID: actor.ActorID(tc.sender)}, Kind: message.KindRequest, Type: HandleCall, Payload: body})
			sys := &testSys{msgs: []actorbase.Msg{m}}
			def := HandleDef(NewHub(), "body", HandleConfig{Host: "host", Words: map[string]Word{}, Drivers: tc.drivers}, tc.members)
			proc, err := def.New()
			if err != nil {
				t.Fatal(err)
			}
			_ = proc(sys)
			if sys.failure != tc.want {
				t.Fatalf("failure=%q want %q", sys.failure, tc.want)
			}
		})
	}
}
func TestDrivePreservesAudienceActionAndLocalAuthority(t *testing.T) {
	expires := int64(12345)
	env := message.Envelope{ChannelID: "foreign", Sender: message.Sender{ID: "tool:foreign:1"}, Kind: message.KindRequest, Type: "echo", Audience: message.Audience{"tool:target:1"}, Payload: json.RawMessage(`{}`), ExpiresAt: &expires}
	sys := &testSys{}
	_, _ = actLocal(context.Background(), sys, "host", Request{Envelope: wireEnvelope(env), Await: true})
	if sys.caller.Channel != "host" || sys.caller.Actor != sys.Self() || sys.callSpec.ExpiresAt != &expires {
		t.Fatalf("foreign authority or deadline lost: %+v %+v", sys.caller, sys.callSpec)
	}
	if _, err := actLocal(context.Background(), sys, "host", Request{Envelope: wireEnvelope(env)}); err != nil {
		t.Fatal(err)
	}
	env.Kind = message.KindEvent
	if _, err := actLocal(context.Background(), sys, "host", Request{Envelope: wireEnvelope(env)}); err != nil {
		t.Fatal(err)
	}
	if len(sys.posts) != 1 || len(sys.events) != 1 || sys.posts[0].Audience[0] != "tool:target:1" || sys.events[0].Audience[0] != "tool:target:1" {
		t.Fatal("targeted post/emit collapsed into call or broadcast")
	}
}
func TestWordsProjectionDoesNotExposeTargets(t *testing.T) {
	cfg, err := ParseHandleConfig(json.RawMessage(`{"host":"host","words":{"echo":{"schema":{"type":"object"},"target":"private-worker"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(cfg.ManifestWords())
	var words map[string]map[string]any
	_ = json.Unmarshal(raw, &words)
	if _, ok := words["echo"]["target"]; ok {
		t.Fatal("internal target leaked")
	}
	if _, ok := words["echo"]["input_schema"]; !ok {
		t.Fatal("schema lost")
	}
}
