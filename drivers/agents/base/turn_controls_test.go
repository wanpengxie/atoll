package base

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type captureRuntime struct {
	starts   []runtimeproto.StartCommand
	controls []runtimeproto.ControlCommand
	readyOps []runtimeproto.OpID
	snapshot *driverproto.OptionsSnapshot
}

func (r *captureRuntime) Options() (driverproto.OptionsSnapshot, bool) {
	if r.snapshot == nil {
		return driverproto.OptionsSnapshot{}, false
	}
	return driverproto.CloneOptionsSnapshot(*r.snapshot), true
}

func (r *captureRuntime) Start(v runtimeproto.StartCommand) error {
	r.starts = append(r.starts, v)
	return nil
}
func (r *captureRuntime) Control(v runtimeproto.ControlCommand) error {
	r.controls = append(r.controls, v)
	return nil
}
func (*captureRuntime) Terminate() error { return nil }
func (r *captureRuntime) EnsureReady(op runtimeproto.OpID) error {
	r.readyOps = append(r.readyOps, op)
	return nil
}
func (*captureRuntime) Close() {}

func TestAgentOptionsUsesOrdinaryRequestAndReturnsGenerationSnapshot(t *testing.T) {
	l, sys, rt := newV7Loop(t, nil)
	l.handleIntake(v7Request("options", TypeOptions, "caller", `{}`))
	if len(rt.readyOps) != 1 || l.state.Running == nil || l.state.Running.Kind != book.ActionOptions {
		t.Fatalf("readyOps=%v action=%+v", rt.readyOps, l.state.Running)
	}
	want := driverproto.OptionsSnapshot{
		Provider: "codex", Source: driverproto.OptionsSourceNative,
		Models:  []driverproto.ModelOption{{Value: "gpt-new", Efforts: []driverproto.EffortOption{{Value: "high"}}}},
		Default: driverproto.TurnOptions{Model: "gpt-new", Effort: "high"},
		Current: driverproto.TurnOptions{Model: "gpt-new", Effort: "high"},
		Client:  driverproto.ClientInfo{Name: "codex", Current: "1.2.3", Latest: "1.3.0", UpdateStatus: driverproto.UpdateAvailable},
	}
	l.onReadyDone(runtimeEvent{kind: evReadyDone, op: rt.readyOps[0], ready: runtimeproto.ReadyResult{Ready: true, Options: want}})
	terminal := sys.terminal("options")
	if len(terminal) != 1 || terminal[0].fail || !reflect.DeepEqual(terminal[0].value, want) {
		t.Fatalf("terminal=%#v want=%#v", terminal, want)
	}
	if l.state.Running != nil || l.state.Requests["options"] != nil {
		t.Fatalf("options action leaked: action=%+v request=%+v", l.state.Running, l.state.Requests["options"])
	}
}

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
	add("new", runtimeproto.TurnNew)
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
	if len(rt.starts) != 4 {
		t.Fatalf("batches=%d want 4", len(rt.starts))
	}
	if got := []int{len(rt.starts[0].Messages), len(rt.starts[1].Messages), len(rt.starts[2].Messages), len(rt.starts[3].Messages)}; got[0] != 2 || got[1] != 0 || got[2] != 0 || got[3] != 1 {
		t.Fatalf("batch sizes=%v want [2 0 0 1]", got)
	}
	if rt.starts[0].Kind != runtimeproto.TurnChat || rt.starts[1].Kind != runtimeproto.TurnCompact || rt.starts[2].Kind != runtimeproto.TurnNew || rt.starts[3].Kind != runtimeproto.TurnChat {
		t.Fatalf("batch kinds=%v/%v/%v/%v", rt.starts[0].Kind, rt.starts[1].Kind, rt.starts[2].Kind, rt.starts[3].Kind)
	}
}

func TestBatchingKeepsAttributedCallersSeparate(t *testing.T) {
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
	add := func(id string, caller harness.Caller) {
		requestID := book.RequestID(id)
		l.state.Requests[requestID] = &book.Request{ID: requestID, Input: runtimeproto.Input{SourceID: id, Text: id, Caller: caller}}
		l.state.Buffer = append(l.state.Buffer, requestID)
	}
	callerA := harness.Caller{Channel: "a", Actor: "agent:alice:1"}
	callerB := harness.Caller{Channel: "b", Actor: "agent:alice:1"}
	add("a1", callerA)
	add("a2", callerA)
	add("b1", callerB)
	for len(l.state.Buffer) > 0 {
		l.startNext()
		owner := l.state.Turn.Owner
		l.clearReceipt(receiptKey("start", uint64(l.state.Turn.StartOp)))
		l.clearTurn()
		l.state.RemoveRequest(owner)
	}
	if len(rt.starts) != 2 || len(rt.starts[0].Messages) != 2 || len(rt.starts[1].Messages) != 1 {
		t.Fatalf("caller batches=%+v", rt.starts)
	}
}

func TestAgentVocabularyRejectsLegacyAndUnknownWords(t *testing.T) {
	d := definition{controls: map[string]struct{}{TypeAsk: {}, TypeContext: {}}}
	if !d.supports(TypeAsk) || d.supports("chat.text") || d.supports("agent.unknown") {
		t.Fatalf("agent vocabulary did not stay closed")
	}
}

func TestSelectValidatesStablePayloadShape(t *testing.T) {
	selections := []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, {Model: "m1", Effort: "high"}, {Model: "m2", Effort: "low"}}
	l := &agentLoop{def: definition{cfg: Config{Runtime: runtimeproto.Spec{Selections: selections}}}, options: selections[1]}
	tests := []struct {
		name, payload, code string
		want                runtimeproto.TurnOptions
	}{
		{name: "exact", payload: `{"model":"m2","effort":"low"}`, want: selections[2]},
		{name: "model only leaves effort optional", payload: `{"model":"m1"}`, want: runtimeproto.TurnOptions{Model: "m1"}},
		{name: "effort only is invalid", payload: `{"effort":"low"}`, code: "invalid_args"},
		{name: "catalog validation belongs to runtime", payload: `{"model":"m2","effort":"high"}`, want: runtimeproto.TurnOptions{Model: "m2", Effort: "high"}},
		{name: "unknown field is invalid", payload: `{"model":"m1","future":true}`, code: "invalid_args"},
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
	if _, code, _ := l.validateSelection(json.RawMessage(`{"model":"m1"}`)); code != "" {
		t.Fatalf("stable validation unexpectedly depends on fallback table: code=%q", code)
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
}

type forkPending struct{ terminal actorbase.Msg }

func (p forkPending) RequestID() message.ID { return p.terminal.ParentID }
func (forkPending) Progress() <-chan actorbase.Msg {
	out := make(chan actorbase.Msg)
	close(out)
	return out
}
func (p forkPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return p.terminal, nil
}
func (forkPending) Cancel() error { return nil }

type forkSys struct {
	actorbase.Sys
	mu      sync.Mutex
	done    chan struct{}
	cause   message.Cause
	target  actor.ActorID
	word    string
	payload any
	reply   any
	fail    string
}

func (*forkSys) Self() actor.ActorID { return "agent:clone-source:1" }
func (s *forkSys) Call(cause message.Cause, app harness.Context, target actor.ActorID, word string, payload any) (actorbase.Pending, error) {
	s.mu.Lock()
	s.cause, s.target, s.word, s.payload = cause, target, word, payload
	s.mu.Unlock()
	terminal := actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ParentID: "member-create", Payload: json.RawMessage(`{"status":"completed","member":"agent:clone-source:2"}`)})
	return forkPending{terminal: terminal}, nil
}
func (s *forkSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.mu.Lock()
	s.reply = value
	s.mu.Unlock()
	close(s.done)
	return "reply", nil
}
func (s *forkSys) Fail(_ actorbase.Msg, code, _ string, _ ...map[string]any) (message.ID, error) {
	s.mu.Lock()
	s.fail = code
	s.mu.Unlock()
	close(s.done)
	return "fail", nil
}

func (s *forkSys) snapshot() (message.Cause, actor.ActorID, string, any, any, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause, s.target, s.word, s.payload, s.reply, s.fail
}

func (s *turnControlSys) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.mu.Lock()
	s.replies = append(s.replies, value)
	s.mu.Unlock()
	return "reply", nil
}
func (s *turnControlSys) Fail(actorbase.Msg, string, string, ...map[string]any) (message.ID, error) {
	return "fail", nil
}
func (*turnControlSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }
func (*turnControlSys) Self() actor.ActorID                                { return "agent:test" }

func baseRequest(id, typ string) actorbase.Msg {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), Sender: message.Sender{ID: "caller"}, Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(`{}`)})
}

func TestTurnEndedCarriesUsageOnlyInTerminalReply(t *testing.T) {
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
	reply := sys.replies[0]
	sys.mu.Unlock()
	value := reply.(map[string]any)
	if !jsonEqual(t, value["usage"], usageValue(usage)) {
		t.Fatalf("reply usage=%v", value["usage"])
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

func TestAgentForkCallsTheDoorWithItsOwnDeclaration(t *testing.T) {
	sys := &forkSys{done: make(chan struct{})}
	vault := effectcap.NewVault()
	exec := newExecutor(sys, vault)
	msg := baseRequest("fork", TypeFork)
	exec.install(msg)
	l := &agentLoop{sys: sys, exec: exec, vault: vault}
	l.handleFork(msg)
	select {
	case <-sys.done:
	case <-time.After(time.Second):
		t.Fatal("fork did not finish")
	}
	cause, target, word, captured, reply, fail := sys.snapshot()
	payload, _ := captured.(map[string]any)
	if target != actor.SystemActorID || word != "system.member.create" || payload["decl_id"] != "clone-source" {
		t.Fatalf("call target=%q word=%q payload=%v", target, word, captured)
	}
	// The door call is made to serve the fork request, so it continues that
	// request's errand instead of opening one of its own.
	if !cause.Stated() || cause.IsRoot() {
		t.Fatalf("fork call cause stated=%v root=%v, want a cause naming the fork request", cause.Stated(), cause.IsRoot())
	}
	raw, ok := reply.(json.RawMessage)
	if !ok || string(raw) != `{"status":"completed","member":"agent:clone-source:2"}` {
		t.Fatalf("reply=%T/%v fail=%q", reply, reply, fail)
	}
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	aRaw, _ := json.Marshal(a)
	bRaw, _ := json.Marshal(b)
	return string(aRaw) == string(bRaw)
}

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// agent.options is a read. With a live generation it answers from the catalog
// mirror and never touches the control slot — the slot serialises actions that
// change the current turn, and an options request parked there once froze
// every steer and interrupt until the turn ended.
func TestAgentOptionsAnswersFromLiveCatalogWithoutTouchingTheSlot(t *testing.T) {
	l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
	rt.snapshot = &driverproto.OptionsSnapshot{Provider: "claude", Source: driverproto.OptionsSourceNative, Models: []driverproto.ModelOption{{Value: "fable"}}}
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("options", TypeOptions, "caller", `{}`))
	if got := sys.terminal("options"); len(got) != 1 || got[0].fail {
		t.Fatalf("options terminal=%v", got)
	}
	if got := sys.terminal("options")[0].value.(driverproto.OptionsSnapshot); got.Provider != "claude" || len(got.Models) != 1 || got.Models[0].Value != "fable" {
		t.Fatalf("snapshot=%+v", got)
	}
	if len(rt.readyOps) != 0 || l.state.Running != nil || l.state.Pending != nil {
		t.Fatalf("readyOps=%v running=%+v pending=%+v: a read must not enter the slot", rt.readyOps, l.state.Running, l.state.Pending)
	}
	// And a steer right after it is dispatched at once.
	l.handleIntake(v7Request("q", TypeAsk, "caller", `{"text":"later"}`))
	l.handleIntake(v7Request("steer", TypeSteer, "caller", `{"target":"q"}`))
	if len(rt.controls) != 1 || rt.controls[0].Kind != runtimeproto.ControlSteer {
		t.Fatalf("controls=%+v, want the steer dispatched immediately", rt.controls)
	}
}

// A steer waiting for the slot yields to an interrupt: the target goes back to
// the queue with its buttons, and the steer word says why it did not apply.
func TestInterruptSupersedesAPendingSteer(t *testing.T) {
	l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true, runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("q", TypeAsk, "caller", `{"text":"queued work"}`))
	l.handleIntake(v7Request("steer", TypeSteer, "caller", `{"target":"q"}`))
	// Park the steer: pretend the slot is busy so it stays Pending.
	if l.state.Running == nil {
		t.Fatalf("running=%+v, want the steer dispatched", l.state.Running)
	}
	l.state.Pending, l.state.Running = l.state.Running, nil
	rt.controls = nil
	l.handleIntake(v7Request("stop", TypeInterrupt, "caller", `{}`))
	if got := sys.terminal("stop"); len(got) != 1 || got[0].fail {
		t.Fatalf("interrupt terminal=%v", got)
	}
	if word := sys.terminal("steer"); len(word) != 1 || !word[0].fail || word[0].code != "superseded" {
		t.Fatalf("steer terminal=%v, want failed superseded", word)
	}
	if row := l.state.Requests["q"]; row == nil || row.Location != book.Buffered || l.state.IndexInBuffer("q") < 0 {
		t.Fatalf("target=%+v buffer=%v, want it back in the queue", row, l.state.Buffer)
	}
	if len(rt.controls) != 1 || rt.controls[0].Kind != runtimeproto.ControlInterrupt {
		t.Fatalf("controls=%+v, want the interrupt dispatched", rt.controls)
	}
}
