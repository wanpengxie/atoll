package svcactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type svcPending struct{ msg actorbase.Msg }

func (p svcPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) { return p.msg, nil }
func (svcPending) Cancel() error                                                { return nil }

type svcSys struct {
	actorbase.Sys
	target  actor.ActorID
	word    string
	payload json.RawMessage
	emit    behavior.EventSpec
	fail    string
	recv    []actorbase.Msg
	life    context.Context
}

func (s *svcSys) Call(target actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	s.target, s.word = target, word
	s.payload, _ = json.Marshal(payload)
	env := message.Envelope{ID: "terminal", ParentID: "local-request", Payload: json.RawMessage(`{"status":"completed","value":{"ok":true}}`)}
	return svcPending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)}, nil
}
func (s *svcSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	s.emit = spec
	return "audit", nil
}
func (s *svcSys) Recv() (actorbase.Msg, error) {
	if len(s.recv) == 0 {
		return actorbase.Msg{}, errors.New("done")
	}
	msg := s.recv[0]
	s.recv = s.recv[1:]
	return msg, nil
}
func (s *svcSys) Fail(_ actorbase.Msg, code, _ string) (message.ID, error) {
	s.fail = code
	return "fail", nil
}
func (s *svcSys) Life() context.Context {
	if s.life != nil {
		return s.life
	}
	return context.Background()
}

func svcDeps(class string) Deps {
	return Deps{
		Self: "target", Core: "c0", RegistrarClass: "registrar",
		Endpoints:     func(context.Context) ([]Endpoint, error) { return []Endpoint{{Name: "work", Receiver: "decl"}}, nil },
		Instances:     func(context.Context, string) ([]actor.ActorID, error) { return []actor.ActorID{"receiver"}, nil },
		Parent:        func(context.Context) (channel.ID, error) { return "parent", nil },
		ReceiverClass: func(context.Context, string) (string, error) { return class, nil },
		Card:          func(context.Context, channel.ID) (introspect.Describe, error) { return introspect.Describe{}, nil },
		Audit:         func(context.Context, map[string]any) error { return nil },
	}
}

func TestSvcactorDispatchWrapsOriginOnlyForRegistrarAndAuditsLocalRequest(t *testing.T) {
	deps := svcDeps("registrar")
	sys := &svcSys{}
	var auditRaw json.RawMessage
	deps.Audit = func(_ context.Context, payload map[string]any) error {
		auditRaw, _ = json.Marshal(payload)
		return nil
	}
	req := peerproto.Request{Origin: peerproto.Origin{Channel: "caller", Actor: "alice", RequestID: "remote-request"}, Type: "work", Payload: json.RawMessage(`{"x":1}`)}
	result := dispatch(context.Background(), sys, deps, "caller", req)
	if result.Fail != nil || string(result.Body) != `{"value":{"ok":true}}` {
		t.Fatalf("result=%+v", result)
	}
	var wrapped struct {
		Origin peerproto.Origin `json:"origin"`
		Args   json.RawMessage  `json:"args"`
	}
	if err := json.Unmarshal(sys.payload, &wrapped); err != nil || wrapped.Origin != req.Origin || string(wrapped.Args) != string(req.Payload) {
		t.Fatalf("registrar payload=%s err=%v", sys.payload, err)
	}
	var audit struct {
		Origin         peerproto.Origin `json:"origin"`
		Type           string           `json:"type"`
		LocalRequestID message.ID       `json:"local_request_id"`
	}
	if err := json.Unmarshal(auditRaw, &audit); err != nil || audit.Origin != req.Origin || audit.Type != "work" || audit.LocalRequestID != "local-request" {
		t.Fatalf("audit=%s err=%v", auditRaw, err)
	}
}

func TestSvcactorDispatchLeavesBusinessPayloadByteEquivalent(t *testing.T) {
	deps := svcDeps("codex")
	sys := &svcSys{}
	payload := json.RawMessage(`{"body":{"origin":"business-value"},"n":1}`)
	result := dispatch(context.Background(), sys, deps, "caller", peerproto.Request{Origin: peerproto.Origin{Channel: "caller", Actor: "alice", RequestID: "r"}, Type: "work", Payload: payload})
	if result.Fail != nil || string(sys.payload) != string(payload) {
		t.Fatalf("result=%+v delivered=%s want=%s", result, sys.payload, payload)
	}
}

func TestSvcactorRejectsBeforeWritingTargetLedger(t *testing.T) {
	cases := []struct {
		name, caller, code string
		req                peerproto.Request
		deps               Deps
	}{
		{name: "bad origin", caller: "caller", code: "bad_origin", req: peerproto.Request{Origin: peerproto.Origin{Channel: "forged"}, Type: "work"}, deps: svcDeps("codex")},
		{name: "unknown endpoint", caller: "caller", code: "endpoint_not_found", req: peerproto.Request{Origin: peerproto.Origin{Channel: "caller"}, Type: "missing"}, deps: svcDeps("codex")},
		{name: "foreign management", caller: "foreign", code: "forbidden", req: peerproto.Request{Origin: peerproto.Origin{Channel: "foreign"}, Type: "channel.remove_actor"}, deps: svcDeps("codex")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sys := &svcSys{}
			result := dispatch(context.Background(), sys, tc.deps, channel.ID(tc.caller), tc.req)
			if result.Fail == nil || result.Fail.Code != tc.code || sys.target != "" || sys.emit.Type != "" {
				t.Fatalf("result=%+v target=%q audit=%+v", result, sys.target, sys.emit)
			}
		})
	}
}

func TestSvcactorRejectsMembraneLocalRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: "request", ChannelID: "target", Kind: message.KindRequest, Type: "work"})
	sys := &svcSys{recv: []actorbase.Msg{msg}, life: ctx}
	deps := svcDeps("codex")
	deps.Port = NewPort()
	deps.Card = func(context.Context, channel.ID) (introspect.Describe, error) { return introspect.Describe{}, nil }
	_ = serve(sys, deps)
	if sys.fail != "external-facing" {
		t.Fatalf("fail=%q", sys.fail)
	}
}
