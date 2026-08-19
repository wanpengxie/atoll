package svcactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type svcPending struct{ msg actorbase.Msg }

func (p svcPending) RequestID() message.ID { return p.msg.ParentID }
func (p svcPending) Progress() <-chan actorbase.Msg {
	out := make(chan actorbase.Msg)
	close(out)
	return out
}
func (p svcPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) { return p.msg, nil }
func (svcPending) Cancel() error                                                { return nil }

type svcSys struct {
	actorbase.Sys
	caller  harness.Caller
	target  actor.ActorID
	word    string
	payload json.RawMessage
	callErr error
	reply   any
	fail    string
}

type memoryState struct {
	values map[resource.ResourceID][]byte
}

func newMemoryState() *memoryState { return &memoryState{values: map[resource.ResourceID][]byte{}} }
func (s *memoryState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	raw, ok := s.values[id]
	if !ok {
		return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	return accessdoor.Outcome{Found: true, Value: append([]byte(nil), raw...)}, nil
}
func (s *memoryState) Put(id resource.ResourceID, raw []byte) (accessdoor.Outcome, error) {
	s.values[id] = append([]byte(nil), raw...)
	return accessdoor.Outcome{}, nil
}
func (s *memoryState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	delete(s.values, id)
	return accessdoor.Outcome{}, nil
}

type materializeSys struct {
	actorbase.Sys
	ctx   context.Context
	state *memoryState
	calls int
}

func (s *materializeSys) Life() context.Context        { return s.ctx }
func (s *materializeSys) State() actorbase.StateHandle { return s.state }
func (*materializeSys) Recv() (actorbase.Msg, error)   { return actorbase.Msg{}, context.Canceled }
func (s *materializeSys) Call(_ actor.ActorID, _ string, _ any) (actorbase.Pending, error) {
	s.calls++
	env := message.Envelope{Payload: json.RawMessage(`{"status":"completed","class":"echo","interfaces":["actor"],"capabilities":{},"words":{"echo.say":{"description":"live echo"},"echo.alt":{"description":"live alternate"}}}`)}
	return svcPending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)}, nil
}

func (s *svcSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.reply = value
	return "reply", nil
}
func (s *svcSys) Fail(_ actorbase.Msg, code, _ string) (message.ID, error) {
	s.fail = code
	return "fail", nil
}

func (s *svcSys) CallFor(caller harness.Caller, target actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	s.caller, s.target, s.word = caller, target, word
	s.payload, _ = json.Marshal(payload)
	if s.callErr != nil {
		return nil, s.callErr
	}
	env := message.Envelope{ID: "terminal", ParentID: "local-request", Payload: json.RawMessage(`{"status":"completed","value":{"ok":true}}`)}
	return svcPending{msg: actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env)}, nil
}

func serviceDeps(self channel.ID) Deps {
	return Deps{
		Self: self, Core: "c0",
		Members: Members{
			IsActive: func(context.Context, actor.ActorID) (bool, error) { return true, nil },
			ActorFacts: func(context.Context, actor.ActorID) (MemberFacts, bool, error) {
				return MemberFacts{Kind: actor.KindTool}, true, nil
			},
			FirstActiveAgent: func(context.Context) (actor.ActorID, bool, error) { return "agent:default:1", true, nil },
		},
		Audit: func(context.Context, map[string]any) error { return nil },
	}
}

func TestDispatchPreservesBodyAndEffectiveCaller(t *testing.T) {
	deps := serviceDeps("target")
	s := &service{deps: deps, table: ServiceTable{Endpoints: map[string]actor.ActorID{"work": "tool:worker:1"}}}
	sys := &svcSys{}
	req := channel.Request{From: channel.From{Channel: "caller", Actor: "agent:alice:1", RequestID: "remote"}, Type: "work", Payload: json.RawMessage(`{"origin":"business","n":1}`)}
	result := s.dispatch(context.Background(), sys, "caller", req, nil)
	if result.Fail != nil || string(result.Body) != `{"value":{"ok":true}}` {
		t.Fatalf("result=%+v", result)
	}
	if sys.target != "tool:worker:1" || sys.word != "work" || string(sys.payload) != string(req.Payload) {
		t.Fatalf("target=%q word=%q payload=%s", sys.target, sys.word, sys.payload)
	}
	if sys.caller.Channel != "caller" || sys.caller.Actor != "agent:alice:1" {
		t.Fatalf("caller=%+v", sys.caller)
	}
}

func TestC0SpaceWordRoutesToRegistrarWithoutAServiceTable(t *testing.T) {
	deps := serviceDeps("c0")
	s := &service{deps: deps, table: emptyTable()}
	sys := &svcSys{}
	req := channel.Request{From: channel.From{Channel: "ordinary", Actor: "agent:alice:1", RequestID: "remote"}, Type: "system.channel.get", Payload: json.RawMessage(`{"channel_id":"ordinary"}`)}
	result := s.dispatch(context.Background(), sys, "ordinary", req, nil)
	if result.Fail != nil || sys.target != "system:registrar" {
		t.Fatalf("result=%+v target=%q", result, sys.target)
	}
}

func TestUnknownEndpointFailsBeforeWriting(t *testing.T) {
	deps := serviceDeps("target")
	s := &service{deps: deps, table: emptyTable()}
	sys := &svcSys{}
	result := s.dispatch(context.Background(), sys, "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: "missing"}, nil)
	if result.Fail == nil || result.Fail.Code != "endpoint_not_found" || sys.target != "" {
		t.Fatalf("result=%+v target=%q", result, sys.target)
	}
}

func TestStructuralDispatchBranchesStayClosed(t *testing.T) {
	t.Run("c0 may operate a remote membrane", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		sys := &svcSys{}
		result := s.dispatch(context.Background(), sys, "c0", channel.Request{From: channel.From{Channel: "c0", Actor: "system:registrar:1"}, Type: message.TypeSystemMemberDelete, Payload: json.RawMessage(`{"member":"tool:x:1"}`)}, nil)
		if result.Fail != nil || sys.target != actor.SystemActorID {
			t.Fatalf("result=%+v target=%q", result, sys.target)
		}
	})
	t.Run("ordinary channel cannot operate another membrane", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		result := s.dispatch(context.Background(), &svcSys{}, "other", channel.Request{From: channel.From{Channel: "other"}, Type: message.TypeSystemMemberDelete}, nil)
		if result.Fail == nil || result.Fail.Code != string(channel.GateEndpointNotFound) {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("ordinary space route resolves no local registrar", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		sys := &svcSys{callErr: &actorbase.TargetResolveError{Code: "not_found", Target: "system:registrar"}}
		result := s.dispatch(context.Background(), sys, "other", channel.Request{From: channel.From{Channel: "other"}, Type: message.TypeSystemChannelList}, nil)
		if result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(channel.GateEndpointNotFound) {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("missing service agent is explicit", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		result := s.dispatch(context.Background(), &svcSys{}, "other", channel.Request{From: channel.From{Channel: "other"}, Type: "agent.ask"}, nil)
		if result.Fail == nil || result.Fail.Code != string(channel.GateNoServiceAgent) {
			t.Fatalf("result=%+v", result)
		}
	})
}

func TestServiceAgentDispatchCoversNullDefaultAndNamedStates(t *testing.T) {
	named := "agent:named:1"
	defaultValue := "default"
	for _, test := range []struct {
		name  string
		agent *string
		want  actor.ActorID
		fail  channel.GateCode
	}{
		{name: "null", fail: channel.GateNoServiceAgent},
		{name: "default", agent: &defaultValue, want: "agent:default:1"},
		{name: "named", agent: &named, want: "agent:named:1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &service{deps: serviceDeps("target"), table: ServiceTable{SvcAgent: test.agent, Endpoints: map[string]actor.ActorID{}}}
			sys := &svcSys{}
			result := s.dispatch(context.Background(), sys, "caller", channel.Request{From: channel.From{Channel: "caller", Actor: "human:alice:1"}, Type: "agent.ask", Payload: json.RawMessage(`{"text":"hello"}`)}, nil)
			if test.fail != "" {
				if result.Fail == nil || result.Fail.Code != string(test.fail) {
					t.Fatalf("result=%+v", result)
				}
				return
			}
			if result.Fail != nil || sys.target != test.want {
				t.Fatalf("result=%+v target=%q", result, sys.target)
			}
		})
	}
}

func TestFailurePropagationTableMatchesA9FrameCodes(t *testing.T) {
	for _, code := range []channel.GateCode{
		channel.GateEndpointNotFound,
		channel.GateReceiverInactive,
		channel.GateBadOrigin,
		channel.GateForbidden,
		channel.GateNoServiceAgent,
		channel.GateChannelUnavailable,
	} {
		result := gateFailure(code, "gate detail")
		if result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(code) {
			t.Fatalf("gate %q result=%+v", code, result)
		}
	}
	for _, test := range []struct {
		name string
		raw  string
		code string
	}{
		{name: "receiver application failure", raw: `{"status":"failed","reason":"receiver_internal_error","error_code":"tool_failed","detail":"bad tool"}`, code: "tool_failed"},
		{name: "receiver death", raw: `{"status":"failed","reason":"receiver_unavailable","detail":"dead"}`, code: "receiver_unavailable"},
		{name: "receiver deadline", raw: `{"status":"failed","reason":"unanswered_timeout","detail":"late"}`, code: "unanswered_timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := terminalResult(json.RawMessage(test.raw))
			if result.Fail == nil || result.Fail.Stage != channel.StageReceiver || result.Fail.Code != test.code {
				t.Fatalf("result=%+v", result)
			}
		})
	}
	if result := closedResult(); result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(channel.GateChannelUnavailable) {
		t.Fatalf("closed port result=%+v", result)
	}
}

func TestServiceManifestMaterializesOnceSurvivesRestartAndUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state := newMemoryState()
	table := ServiceTable{Endpoints: map[string]actor.ActorID{"echo.say": "tool:echo:1"}}
	raw, _ := json.Marshal(table)
	_, _ = state.Put(ServiceStateKey, raw)
	sys := &materializeSys{ctx: ctx, state: state}
	deps := serviceDeps("target")
	deps.Port = NewPort()
	defer deps.Port.Close()

	first := &service{deps: deps, table: emptyTable()}
	if err := first.serve(sys); !errors.Is(err, context.Canceled) || sys.calls != 1 {
		t.Fatalf("first serve err=%v describe calls=%d", err, sys.calls)
	}
	words, found, err := readDynamicWords(state)
	if err != nil || !found || words["echo.say"].Description != "live echo" {
		t.Fatalf("first words=%+v found=%v err=%v", words, found, err)
	}

	restarted := &service{deps: deps, table: emptyTable()}
	if err := restarted.serve(sys); !errors.Is(err, context.Canceled) || sys.calls != 1 {
		t.Fatalf("restart err=%v describe calls=%d", err, sys.calls)
	}

	updated := ServiceTable{Endpoints: map[string]actor.ActorID{"echo.alt": "tool:echo:1"}}
	if err := restarted.materialize(sys, updated); err != nil {
		t.Fatal(err)
	}
	words, found, err = readDynamicWords(state)
	if err != nil || !found || len(words) != 1 || words["echo.alt"].Description != "live alternate" {
		t.Fatalf("updated words=%+v found=%v err=%v", words, found, err)
	}
}

func TestC0ServiceTableIsEmptyAndImmutable(t *testing.T) {
	s := &service{deps: serviceDeps("c0"), table: emptyTable()}
	request := func(word string, payload string) actorbase.Msg {
		return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
			ID: message.ID(word), ChannelID: "c0", Kind: message.KindRequest, Type: word,
			Sender: message.Sender{Kind: actor.KindAgent, ID: "agent:steward:1"}, Payload: json.RawMessage(`{"body":` + payload + `}`),
		})
	}
	getSys := &svcSys{}
	s.handleMailbox(getSys, request("svcactor.get", `{}`))
	table, ok := getSys.reply.(ServiceTable)
	if !ok || table.SvcAgent != nil || len(table.Endpoints) != 0 {
		t.Fatalf("get=%#v fail=%q", getSys.reply, getSys.fail)
	}
	setSys := &svcSys{}
	s.handleMailbox(setSys, request("svcactor.set", `{"svc_agent":null,"endpoints":{}}`))
	if setSys.fail != "permission_denied" || setSys.reply != nil {
		t.Fatalf("set reply=%#v fail=%q", setSys.reply, setSys.fail)
	}
}

func TestServiceTableValidationRejectsInvalidReceiversAndReservedWords(t *testing.T) {
	tests := []struct {
		name  string
		table ServiceTable
		facts MemberFacts
		live  bool
	}{
		{name: "reserved system word", table: ServiceTable{Endpoints: map[string]actor.ActorID{"system.x": "tool:x:1"}}, facts: MemberFacts{Kind: actor.KindTool}, live: true},
		{name: "reserved agent ask", table: ServiceTable{Endpoints: map[string]actor.ActorID{"agent.ask": "tool:x:1"}}, facts: MemberFacts{Kind: actor.KindTool}, live: true},
		{name: "short receiver", table: ServiceTable{Endpoints: map[string]actor.ActorID{"work.run": "tool:x"}}, facts: MemberFacts{Kind: actor.KindTool}, live: true},
		{name: "inactive receiver", table: ServiceTable{Endpoints: map[string]actor.ActorID{"work.run": "tool:x:1"}}, facts: MemberFacts{Kind: actor.KindTool}},
		{name: "peer receiver", table: ServiceTable{Endpoints: map[string]actor.ActorID{"work.run": "peer:x:1"}}, facts: MemberFacts{Kind: actor.KindPeer}, live: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := serviceDeps("target")
			deps.Members.ActorFacts = func(context.Context, actor.ActorID) (MemberFacts, bool, error) { return test.facts, test.live, nil }
			s := &service{deps: deps, table: emptyTable()}
			if err := s.validateTable(context.Background(), test.table); err == nil {
				t.Fatalf("table accepted: %+v", test.table)
			}
		})
	}
	for _, kind := range []actor.Kind{actor.KindHuman, actor.KindTool} {
		t.Run("service agent "+string(kind), func(t *testing.T) {
			deps := serviceDeps("target")
			deps.Members.ActorFacts = func(context.Context, actor.ActorID) (MemberFacts, bool, error) {
				return MemberFacts{Kind: kind}, true, nil
			}
			s := &service{deps: deps, table: emptyTable()}
			id := "agent:x:1"
			if err := s.validateTable(context.Background(), ServiceTable{SvcAgent: &id, Endpoints: map[string]actor.ActorID{}}); err == nil {
				t.Fatalf("svc_agent kind %s accepted", kind)
			}
		})
	}
}

type emptyState struct{}

func (emptyState) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
}
func (emptyState) Put(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (emptyState) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type blockedPending struct {
	started chan struct{}
	release chan struct{}
}

func (blockedPending) RequestID() message.ID { return "blocked" }
func (blockedPending) Progress() <-chan actorbase.Msg {
	out := make(chan actorbase.Msg)
	close(out)
	return out
}
func (p blockedPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	close(p.started)
	<-p.release
	env := message.Envelope{Payload: json.RawMessage(`{"status":"completed","ok":true}`)}
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), env), nil
}
func (blockedPending) Cancel() error { return nil }

type concurrentSys struct {
	actorbase.Sys
	ctx     context.Context
	pending blockedPending
}

func (s *concurrentSys) Life() context.Context      { return s.ctx }
func (*concurrentSys) State() actorbase.StateHandle { return emptyState{} }
func (s *concurrentSys) CallFor(harness.Caller, actor.ActorID, string, any) (actorbase.Pending, error) {
	return s.pending, nil
}

func TestBlockedServiceRequestDoesNotBlockDescribe(t *testing.T) {
	port := NewPort()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer port.Close()
	started, release := make(chan struct{}), make(chan struct{})
	deps := serviceDeps("target")
	deps.Port = port
	agentID := "agent:service:1"
	s := &service{deps: deps, table: ServiceTable{SvcAgent: &agentID, Endpoints: map[string]actor.ActorID{}}}
	sys := &concurrentSys{ctx: ctx, pending: blockedPending{started: started, release: release}}
	go s.servePort(sys)
	firstDone := make(chan channel.Result, 1)
	go func() {
		result, _ := port.Call(ctx, "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: "agent.ask"}, nil)
		firstDone <- result
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("service request never blocked")
	}
	describeCtx, stopDescribe := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopDescribe()
	describe, err := port.Call(describeCtx, "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: "actor.describe"}, nil)
	if err != nil || describe.Fail != nil || len(describe.Body) == 0 {
		t.Fatalf("describe=%+v err=%v", describe, err)
	}
	close(release)
	select {
	case result := <-firstDone:
		if result.Fail != nil {
			t.Fatalf("blocked request result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked request did not finish")
	}
}
