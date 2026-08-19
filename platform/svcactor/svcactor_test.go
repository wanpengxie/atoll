package svcactor

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
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
	state   actorbase.StateHandle
}

type memoryState struct {
	mu     sync.Mutex
	values map[resource.ResourceID][]byte
	putErr error
	puts   int
}

func newMemoryState() *memoryState { return &memoryState{values: map[resource.ResourceID][]byte{}} }
func (s *memoryState) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[id]
	if !ok {
		return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	return accessdoor.Outcome{Found: true, Value: append([]byte(nil), raw...)}, nil
}
func (s *memoryState) Put(id resource.ResourceID, raw []byte) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return accessdoor.Outcome{}, s.putErr
	}
	s.puts++
	s.values[id] = append([]byte(nil), raw...)
	return accessdoor.Outcome{}, nil
}
func (s *memoryState) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, id)
	return accessdoor.Outcome{}, nil
}

func (s *memoryState) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

type materializeSys struct {
	actorbase.Sys
	ctx   context.Context
	state *memoryState
	calls int
	recv  <-chan struct{}
}

func (s *materializeSys) Life() context.Context        { return s.ctx }
func (s *materializeSys) State() actorbase.StateHandle { return s.state }
func (s *materializeSys) Recv() (actorbase.Msg, error) {
	if s.recv != nil {
		<-s.recv
	}
	return actorbase.Msg{}, context.Canceled
}
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

func (s *svcSys) State() actorbase.StateHandle { return s.state }

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
	result := s.dispatch(context.Background(), context.Background(), sys, "caller", req, nil)
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
	result := s.dispatch(context.Background(), context.Background(), sys, "ordinary", req, nil)
	if result.Fail != nil || sys.target != "system:registrar" {
		t.Fatalf("result=%+v target=%q", result, sys.target)
	}
}

func TestUnknownEndpointFailsBeforeWriting(t *testing.T) {
	deps := serviceDeps("target")
	s := &service{deps: deps, table: emptyTable()}
	sys := &svcSys{}
	result := s.dispatch(context.Background(), context.Background(), sys, "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: "missing"}, nil)
	if result.Fail == nil || result.Fail.Code != "endpoint_not_found" || sys.target != "" {
		t.Fatalf("result=%+v target=%q", result, sys.target)
	}
}

func TestInternalManifestAndPeerCardRemainSeparateSurfaces(t *testing.T) {
	internal := manifest()
	if internal.Class != "svcactor" || !reflect.DeepEqual(internal.Interfaces, []string{"actor", "svcactor"}) || len(internal.Words) != 2 {
		t.Fatalf("internal manifest=%+v", internal)
	}
	if _, ok := internal.Words["svcactor.set"]; !ok {
		t.Fatalf("internal manifest words=%v", internal.Words)
	}
	if _, ok := internal.Words["svcactor.get"]; !ok {
		t.Fatalf("internal manifest words=%v", internal.Words)
	}
	if _, ok := internal.Words["actor.describe"]; ok {
		t.Fatalf("engine-owned actor.describe was declared as a mailbox word")
	}

	s := &service{deps: serviceDeps("target"), table: emptyTable(), card: channel.Card{Words: map[string]json.RawMessage{"agent.ask": json.RawMessage(`{}`)}}}
	for _, word := range []string{"actor.describe", "svcactor.set", "svcactor.get"} {
		result := s.dispatch(context.Background(), context.Background(), &svcSys{}, "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: word}, nil)
		if result.Fail == nil || result.Fail.Code != string(channel.GateEndpointNotFound) {
			t.Fatalf("peer call %q result=%+v", word, result)
		}
	}
	if card := s.cardSnapshot(); len(card.Words) != 1 || card.Words["agent.ask"] == nil {
		t.Fatalf("peer card=%+v", card)
	}
}

func TestStructuralDispatchBranchesStayClosed(t *testing.T) {
	t.Run("c0 may operate a remote membrane", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		sys := &svcSys{}
		result := s.dispatch(context.Background(), context.Background(), sys, "c0", channel.Request{From: channel.From{Channel: "c0", Actor: "system:registrar:1"}, Type: message.TypeSystemMemberDelete, Payload: json.RawMessage(`{"member":"tool:x:1"}`)}, nil)
		if result.Fail != nil || sys.target != actor.SystemActorID {
			t.Fatalf("result=%+v target=%q", result, sys.target)
		}
	})
	t.Run("ordinary channel cannot operate another membrane", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		result := s.dispatch(context.Background(), context.Background(), &svcSys{}, "other", channel.Request{From: channel.From{Channel: "other"}, Type: message.TypeSystemMemberDelete}, nil)
		if result.Fail == nil || result.Fail.Code != string(channel.GateEndpointNotFound) {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("ordinary space route resolves no local registrar", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		sys := &svcSys{callErr: &actorbase.TargetResolveError{Code: "not_found", Target: "system:registrar"}}
		result := s.dispatch(context.Background(), context.Background(), sys, "other", channel.Request{From: channel.From{Channel: "other"}, Type: message.TypeSystemChannelList}, nil)
		if result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(channel.GateEndpointNotFound) {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("missing service agent is explicit", func(t *testing.T) {
		s := &service{deps: serviceDeps("remote"), table: emptyTable()}
		result := s.dispatch(context.Background(), context.Background(), &svcSys{}, "other", channel.Request{From: channel.From{Channel: "other"}, Type: "agent.ask"}, nil)
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
			result := s.dispatch(context.Background(), context.Background(), sys, "caller", channel.Request{From: channel.From{Channel: "caller", Actor: "human:alice:1"}, Type: "agent.ask", Payload: json.RawMessage(`{"text":"hello"}`)}, nil)
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

func TestNamedServiceAgentMustStillBeActiveAtDispatch(t *testing.T) {
	deps := serviceDeps("target")
	deps.Members.IsActive = func(context.Context, actor.ActorID) (bool, error) { return false, nil }
	named := "agent:named:1"
	s := &service{deps: deps, table: ServiceTable{SvcAgent: &named, Endpoints: map[string]actor.ActorID{}}}
	result := s.dispatch(context.Background(), context.Background(), &svcSys{}, "caller", channel.Request{
		From: channel.From{Channel: "caller", Actor: "human:alice:1"}, Type: "agent.ask",
	}, nil)
	if result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(channel.GateReceiverInactive) {
		t.Fatalf("inactive named service agent result=%+v", result)
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
	state := newMemoryState()
	table := ServiceTable{Endpoints: map[string]actor.ActorID{"echo.say": "tool:echo:1"}}
	raw, _ := BootstrapState(table)
	_, _ = state.Put(ServiceStateKey, raw)
	state.puts = 0
	stopFirst := make(chan struct{})
	sys := &materializeSys{ctx: context.Background(), state: state, recv: stopFirst}
	deps := serviceDeps("target")
	deps.Port = NewPort()
	defer deps.Port.Close()

	first := &service{deps: deps, table: emptyTable()}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.serve(sys) }()
	deadline := time.Now().Add(time.Second)
	for state.putCount() != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	close(stopFirst)
	if err := <-firstDone; !errors.Is(err, context.Canceled) || sys.calls != 1 {
		t.Fatalf("first serve err=%v describe calls=%d puts=%d", err, sys.calls, state.puts)
	}
	persisted, found, err := readService(state)
	var echoSpec introspect.WordSpec
	if err == nil && found && persisted.Card != nil {
		_ = json.Unmarshal(persisted.Card.Words["echo.say"], &echoSpec)
	}
	if err != nil || !found || persisted.Card == nil || echoSpec.Description != "live echo" || state.puts != 1 {
		t.Fatalf("first state=%+v spec=%+v found=%v puts=%d err=%v", persisted, echoSpec, found, state.puts, err)
	}

	restarted := &service{deps: deps, table: emptyTable()}
	stopRestart := make(chan struct{})
	sys.recv = stopRestart
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.serve(sys) }()
	close(stopRestart)
	if err := <-restartDone; !errors.Is(err, context.Canceled) || sys.calls != 1 {
		t.Fatalf("restart err=%v describe calls=%d", err, sys.calls)
	}

	updated := ServiceTable{Endpoints: map[string]actor.ActorID{"echo.alt": "tool:echo:1"}}
	updatedCard := restarted.buildCard(sys, updated)
	if err := writeService(state, updated, updatedCard); err != nil {
		t.Fatal(err)
	}
	persisted, found, err = readService(state)
	var altSpec introspect.WordSpec
	if err == nil && found && persisted.Card != nil {
		_ = json.Unmarshal(persisted.Card.Words["echo.alt"], &altSpec)
	}
	if err != nil || !found || persisted.Card == nil || altSpec.Description != "live alternate" || sys.calls != 2 || state.puts != 2 {
		t.Fatalf("updated state=%+v spec=%+v calls=%d puts=%d found=%v err=%v", persisted, altSpec, sys.calls, state.puts, found, err)
	}
}

func TestServiceSetPublishesTableAndCardOnlyAfterOneSuccessfulWrite(t *testing.T) {
	state := newMemoryState()
	oldTable := ServiceTable{Endpoints: map[string]actor.ActorID{"old.run": "tool:old:1"}}
	oldCard := channel.Card{Words: map[string]json.RawMessage{"old.run": json.RawMessage(`{"description":"old"}`)}}
	if err := writeService(state, oldTable, oldCard); err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), state.values[ServiceStateKey]...)
	state.puts = 0
	state.putErr = errors.New("disk full")
	s := &service{deps: serviceDeps("target"), table: cloneTable(oldTable), card: cloneCard(oldCard)}
	sys := &svcSys{state: state}
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "set", ChannelID: "target", Kind: message.KindRequest, Type: "svcactor.set",
		Sender: message.Sender{ID: "agent:owner:1"}, Payload: json.RawMessage(`{"body":{"svc_agent":null,"endpoints":{}}}`),
	})
	s.handleMailbox(sys, msg)
	if sys.fail != "internal_error" || state.puts != 0 || string(state.values[ServiceStateKey]) != string(before) {
		t.Fatalf("fail=%q puts=%d state=%s", sys.fail, state.puts, state.values[ServiceStateKey])
	}
	if got := s.snapshot(); len(got.Endpoints) != 1 || got.Endpoints["old.run"] != "tool:old:1" {
		t.Fatalf("in-memory table changed on failed write: %+v", got)
	}
	if got := s.cardSnapshot(); string(got.Words["old.run"]) != `{"description":"old"}` {
		t.Fatalf("in-memory card changed on failed write: %+v", got)
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

func TestServiceMailboxRejectsUnknownFieldsImmediately(t *testing.T) {
	s := &service{deps: serviceDeps("target"), table: emptyTable(), card: channel.Card{Words: map[string]json.RawMessage{}}}
	sys := &svcSys{}
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "get", ChannelID: "target", Kind: message.KindRequest, Type: "svcactor.get",
		Sender: message.Sender{ID: "agent:member:1"}, Payload: json.RawMessage(`{"body":{"extra":true}}`),
	})
	s.handleMailbox(sys, msg)
	if sys.reply != nil || sys.fail != "invalid_args" {
		t.Fatalf("reply=%#v fail=%q", sys.reply, sys.fail)
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
func (p blockedPending) Wait(ctx context.Context, _ time.Duration) (actorbase.Msg, error) {
	close(p.started)
	select {
	case <-p.release:
	case <-ctx.Done():
		return actorbase.Msg{}, ctx.Err()
	}
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
	s := &service{
		deps: deps, table: ServiceTable{SvcAgent: &agentID, Endpoints: map[string]actor.ActorID{}},
		card: channel.Card{Words: map[string]json.RawMessage{"agent.ask": json.RawMessage(`{"description":"ask"}`)}},
	}
	sys := &concurrentSys{ctx: ctx, pending: blockedPending{started: started, release: release}}
	serveDone := make(chan struct{})
	go func() { s.servePort(ctx, sys); close(serveDone) }()
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
	describe, err := port.Describe(describeCtx, "caller", channel.Describe{From: channel.DescribeFrom{Channel: "caller"}})
	if err != nil || string(describe.Words["agent.ask"]) != `{"description":"ask"}` {
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
	cancel()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("service port generation did not stop")
	}
}

func TestServiceActorStopCancelsInFlightWithChannelUnavailableAndJoins(t *testing.T) {
	baseline := runtime.NumGoroutine()
	port := NewPort()
	defer port.Close()
	life, stopLife := context.WithCancel(context.Background())
	started := make(chan struct{})
	deps := serviceDeps("target")
	deps.Port = port
	agentID := "agent:service:1"
	s := &service{deps: deps, table: ServiceTable{SvcAgent: &agentID, Endpoints: map[string]actor.ActorID{}}}
	sys := &concurrentSys{ctx: life, pending: blockedPending{started: started, release: make(chan struct{})}}
	serveDone := make(chan struct{})
	go func() { s.servePort(life, sys); close(serveDone) }()
	callDone := make(chan channel.Result, 1)
	go func() {
		result, _ := port.Call(context.Background(), "caller", channel.Request{From: channel.From{Channel: "caller"}, Type: "agent.ask"}, nil)
		callDone <- result
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("in-flight service request did not start")
	}
	stopLife()
	select {
	case result := <-callDone:
		if result.Fail == nil || result.Fail.Stage != channel.StageGate || result.Fail.Code != string(channel.GateChannelUnavailable) {
			t.Fatalf("stopped in-flight result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("stopped in-flight request did not terminate")
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("service port and child goroutine did not join")
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := runtime.NumGoroutine(); got > baseline+1 {
		t.Fatalf("goroutines after lifecycle join=%d baseline=%d", got, baseline)
	}
}
