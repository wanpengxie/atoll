package base

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type captureRuntime struct{ starts []runtimeproto.StartCommand }

func (r *captureRuntime) Start(v runtimeproto.StartCommand) error {
	r.starts = append(r.starts, v)
	return nil
}
func (*captureRuntime) Control(runtimeproto.ControlCommand) error { return nil }
func (*captureRuntime) Terminate() error                          { return nil }
func (*captureRuntime) EnsureReady(runtimeproto.OpID) error       { return nil }
func (*captureRuntime) Close()                                    {}

func TestCommandRequestsFormSingleItemBatches(t *testing.T) {
	rt := &captureRuntime{}
	vault := effectcap.NewVault()
	l := &agentLoop{
		def: definition{cfg: Config{BatchMaxCount: 32, ReceiptDeadline: time.Hour}}, state: book.New(),
		exec: &executor{runtime: rt, vault: vault, handles: map[string]actorbase.Msg{}, writers: make(chan struct{}, 1)}, vault: vault,
		receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: testLogger(),
	}
	defer func() {
		for _, timer := range l.receiptTimers {
			timer.Stop()
		}
	}()
	add := func(id string, kind runtimeproto.TurnKind) {
		requestID := book.RequestID(id)
		l.state.Requests[requestID] = &book.Request{ID: requestID, Sender: "caller", Input: runtimeproto.Input{SourceID: id, Text: id}, TurnKind: kind}
		l.state.Buffer = append(l.state.Buffer, requestID)
	}
	add("c1", runtimeproto.TurnChat)
	add("c2", runtimeproto.TurnChat)
	add("compact", runtimeproto.TurnCompact)
	add("c3", runtimeproto.TurnChat)

	for len(l.state.Buffer) > 0 {
		l.startNext()
		if l.state.Turn == nil {
			t.Fatal("batch did not start")
		}
		owner := l.state.Turn.Owner
		l.clearReceipt(receiptKey("start", uint64(l.state.Turn.StartOp)))
		l.clearTurn()
		l.state.RemoveRequest(owner)
	}
	if len(rt.starts) != 3 {
		t.Fatalf("batches=%d want 3", len(rt.starts))
	}
	if got := []int{len(rt.starts[0].Messages), len(rt.starts[1].Messages), len(rt.starts[2].Messages)}; got[0] != 2 || got[1] != 0 || got[2] != 1 {
		t.Fatalf("batch sizes=%v want [2 0 1]", got)
	}
	if rt.starts[0].Kind != runtimeproto.TurnChat || rt.starts[1].Kind != runtimeproto.TurnCompact || rt.starts[2].Kind != runtimeproto.TurnChat {
		t.Fatalf("batch kinds=%v/%v/%v", rt.starts[0].Kind, rt.starts[1].Kind, rt.starts[2].Kind)
	}
}

func TestSelectValidatesAgainstProviderSelections(t *testing.T) {
	selections := []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, {Model: "m1", Effort: "high"}, {Model: "m2", Effort: "low"}}
	l := &agentLoop{def: definition{cfg: Config{Runtime: runtimeproto.Spec{Selections: selections}}}, options: selections[1]}
	tests := []struct {
		name, payload, code string
		want                runtimeproto.TurnOptions
	}{
		{name: "exact", payload: `{"model":"m2","effort":"low"}`, want: selections[2]},
		{name: "model only takes first effort", payload: `{"model":"m1"}`, want: selections[0]},
		{name: "effort only uses current model", payload: `{"effort":"low"}`, want: selections[0]},
		{name: "not in table", payload: `{"model":"m2","effort":"high"}`, code: "invalid_args"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, code, _ := l.validateSelection(json.RawMessage(test.payload))
			if code != test.code || got != test.want {
				t.Fatalf("selection=%+v code=%q want=%+v/%q", got, code, test.want, test.code)
			}
		})
	}
	l.def.cfg.Runtime.Selections = nil
	if _, code, _ := l.validateSelection(json.RawMessage(`{"model":"m1"}`)); code != "type_unsupported" {
		t.Fatalf("provider without table code=%q", code)
	}
}

type turnControlState struct {
	mu    sync.Mutex
	value []byte
	puts  chan struct{}
	gets  int
}

func (s *turnControlState) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.gets == 1 {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	return accessdoor.Outcome{Found: true, Value: append([]byte(nil), s.value...)}, nil
}
func (s *turnControlState) Put(_ resource.ResourceID, value []byte) (accessdoor.Outcome, error) {
	s.mu.Lock()
	s.value = append([]byte(nil), value...)
	s.mu.Unlock()
	select {
	case s.puts <- struct{}{}:
	default:
	}
	return accessdoor.Outcome{}, nil
}
func (*turnControlState) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type turnControlStateSys struct {
	actorbase.Sys
	state *turnControlState
}

func (s turnControlStateSys) State() actorbase.StateHandle { return s.state }
func (turnControlStateSys) Self() actor.ActorID            { return "agent:test" }

func TestSelectPersistsAndReloadsAcrossRun(t *testing.T) {
	state := &turnControlState{puts: make(chan struct{}, 1)}
	sys := turnControlStateSys{state: state}
	want := runtimeproto.TurnOptions{Model: "m2", Effort: "high"}
	persistSelection(sys, want)
	select {
	case <-state.puts:
	case <-time.After(time.Second):
		t.Fatal("selection was not persisted")
	}
	spec := runtimeproto.Spec{Selections: []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, want}, DefaultSelection: 0}
	if got := readSelection(context.Background(), sys, spec); got != want {
		t.Fatalf("reloaded=%+v want=%+v", got, want)
	}
	if state.gets != 2 {
		t.Fatalf("state gets=%d want unknown retry then value", state.gets)
	}
}

type turnControlSys struct {
	actorbase.Sys
	mu      sync.Mutex
	replies []any
	events  []behavior.EventSpec
}

func (s *turnControlSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.mu.Lock()
	s.replies = append(s.replies, value)
	s.mu.Unlock()
	return "reply", nil
}
func (s *turnControlSys) Fail(actorbase.Msg, string, string) (message.ID, error) { return "fail", nil }
func (s *turnControlSys) Emit(event behavior.EventSpec) (message.ID, error) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	return "event", nil
}
func (*turnControlSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }
func (*turnControlSys) Self() actor.ActorID                                { return "agent:test" }

func baseRequest(id, typ string) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), Sender: message.Sender{ID: "caller"}, Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(`{}`)})
}

func TestTurnEndedCarriesUsageIntoActivityAndReply(t *testing.T) {
	sys := &turnControlSys{}
	vault := effectcap.NewVault()
	exec := newExecutor(sys, vault)
	msg := baseRequest("owner", "chat")
	exec.install(msg)
	l := &agentLoop{exec: exec, vault: vault, state: book.New(), receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: testLogger()}
	id := book.RequestID(msg.ID)
	l.state.Requests[id] = &book.Request{ID: id}
	l.state.Turn = &book.Turn{Serial: 7, Phase: book.TurnActive, ID: "turn", Owner: id, AnchorParent: string(msg.ID)}
	usage := runtimeproto.TurnUsage{ContextTokens: 123, ContextWindow: 456, Model: "m", Effort: "high"}
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn", status: runtimeproto.TurnStatusOK, text: "done", usage: usage})
	deadline := time.Now().Add(time.Second)
	for {
		sys.mu.Lock()
		replies := len(sys.replies)
		sys.mu.Unlock()
		if replies == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reply not written")
		}
		time.Sleep(time.Millisecond)
	}
	sys.mu.Lock()
	reply, event := sys.replies[0], sys.events[0]
	sys.mu.Unlock()
	value := reply.(map[string]any)
	if !jsonEqual(t, value["usage"], usageValue(usage)) {
		t.Fatalf("reply usage=%v", value["usage"])
	}
	var activity struct {
		Usage map[string]any `json:"usage"`
	}
	if json.Unmarshal(event.Payload, &activity) != nil || !jsonEqual(t, activity.Usage, usageValue(usage)) {
		t.Fatalf("activity=%s", event.Payload)
	}
}

func TestContextReadReturnsLastUsageWithoutTurn(t *testing.T) {
	sys := &turnControlSys{}
	vault := effectcap.NewVault()
	exec := newExecutor(sys, vault)
	usage := runtimeproto.TurnUsage{ContextTokens: 12, ContextWindow: 34, Model: "m", Effort: "low"}
	l := &agentLoop{def: definition{controls: map[string]struct{}{TypeContext: {}}}, sys: sys, exec: exec, vault: vault, state: book.New(), lastUsage: usage, hasUsage: true}
	l.handleIntake(baseRequest("context", TypeContext))
	deadline := time.Now().Add(time.Second)
	for {
		sys.mu.Lock()
		replies := append([]any(nil), sys.replies...)
		sys.mu.Unlock()
		if len(replies) == 1 {
			if !jsonEqual(t, replies[0], usageValue(usage)) {
				t.Fatalf("context=%v", replies[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("context reply not written")
		}
		time.Sleep(time.Millisecond)
	}
	if l.state.Turn != nil {
		t.Fatal("context read occupied a turn")
	}
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	aRaw, _ := json.Marshal(a)
	bRaw, _ := json.Marshal(b)
	return string(aRaw) == string(bRaw)
}

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }
