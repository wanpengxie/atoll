package base

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

type baseTestSys struct {
	actorbase.Sys
	replies, fails []string
}

func (s *baseTestSys) Reply(_ actorbase.Msg, _ any) (message.ID, error) {
	s.replies = append(s.replies, "reply")
	return "reply", nil
}
func (s *baseTestSys) Fail(_ actorbase.Msg, code, _ string) (message.ID, error) {
	s.fails = append(s.fails, code)
	return "fail", nil
}
func (s *baseTestSys) Progress(actorbase.Msg, string, any) (message.ID, error) {
	return "progress", nil
}

type baseTestRuntime struct{ controls, terminates int }

func (*baseTestRuntime) Start(StartCommand) error       { return nil }
func (r *baseTestRuntime) Control(ControlCommand) error { r.controls++; return nil }
func (r *baseTestRuntime) Terminate() error             { r.terminates++; return nil }
func (*baseTestRuntime) EnsureReady(OpID) error         { return nil }
func (*baseTestRuntime) Close()                         {}
func testItem(id, typ, payload string) *requestItem {
	return newRequestItem(actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(payload)}))
}

func TestSteerInputPhysicallyStripsControlFields(t *testing.T) {
	i := testItem("m", TypeSteer, `{"expected_turn_id":"turn-1","text":"hello"}`)
	out := steerInput(i)
	if out.Type != "" || out.Payload != nil || out.Text != "hello" {
		t.Fatalf("steer input=%+v", out)
	}
}
func TestEffectScopeRevocationStopsFutureAcquire(t *testing.T) {
	s := NewEffectScope("m", "c")
	if _, ok := acquireScope(s); !ok {
		t.Fatal("fresh scope rejected")
	}
	s.Revoke()
	if _, ok := acquireScope(s); ok {
		t.Fatal("revoked scope admitted")
	}
}
func TestStopNeverTerminatesRuntime(t *testing.T) {
	sys := &baseTestSys{}
	rt := &baseTestRuntime{}
	owner := testItem("owner", "chat", `{"text":"x"}`)
	control := testItem("stop", TypeStop, `{}`)
	l := &agentLoop{sys: sys, rt: rt, def: definition{cfg: Config{Runtime: RuntimeSpec{Capabilities: RuntimeCapabilities{Interrupt: true}}}}, book: baseBook{turn: &baseTurn{seq: 1, turnID: "turn", owner: owner, anchor: owner, scope: owner.scope, ops: map[OpID]*turnOp{}}, committing: map[OpID]*commitOp{}, buffer: requestBuffer{maxCount: 8, maxBytes: 1024}}}
	l.acceptAction(actionStop, control)
	if rt.terminates != 0 {
		t.Fatalf("stop called Terminate %d time(s)", rt.terminates)
	}
	if rt.controls != 1 {
		t.Fatalf("cleanup interrupts=%d", rt.controls)
	}
	if len(sys.replies) != 1 {
		t.Fatalf("stop replies=%d", len(sys.replies))
	}
}
func TestExplicitSteerCASFailsBeforeRuntime(t *testing.T) {
	sys := &baseTestSys{}
	rt := &baseTestRuntime{}
	owner := testItem("owner", "chat", `{"text":"x"}`)
	item := testItem("steer", TypeSteer, `{"expected_turn_id":"other","text":"new"}`)
	item.explicitCAS = true
	item.expectedTurn = "other"
	l := &agentLoop{sys: sys, rt: rt, def: definition{cfg: Config{Runtime: RuntimeSpec{Capabilities: RuntimeCapabilities{Steer: true}}}}, book: baseBook{turn: &baseTurn{seq: 1, turnID: "current", owner: owner, scope: owner.scope, ops: map[OpID]*turnOp{}}, committing: map[OpID]*commitOp{}}}
	l.acceptContent(item, true)
	if rt.controls != 0 {
		t.Fatal("mismatched CAS reached runtime")
	}
	if len(sys.fails) != 1 || sys.fails[0] != errorCASMismatch {
		t.Fatalf("failures=%v", sys.fails)
	}
}

func TestRuntimeContractFaultUsesAgentInternalWithoutProviderRecovery(t *testing.T) {
	sys := &baseTestSys{}
	rt := &baseTestRuntime{}
	owner := testItem("owner", "chat", `{"text":"x"}`)
	queued := testItem("queued", "chat", `{"text":"y"}`)
	control := testItem("control", TypeInterrupt, `{}`)
	l := &agentLoop{sys: sys, rt: rt, book: baseBook{turn: &baseTurn{seq: 1, turnID: "turn", owner: owner, scope: owner.scope, ops: map[OpID]*turnOp{}}, running: &baseAction{item: control}, buffer: requestBuffer{items: []*requestItem{queued}, bytes: queued.bytes}, committing: map[OpID]*commitOp{}}}
	_ = l.runtimeFault("receipt_backstop", "stalled")
	if rt.terminates != 0 || rt.controls != 0 {
		t.Fatal("contract fault attempted provider recovery")
	}
	if len(sys.fails) != 3 {
		t.Fatalf("fault terminals=%v", sys.fails)
	}
	for _, code := range sys.fails {
		if code != errorAgentInternal {
			t.Fatalf("fault code=%s", code)
		}
	}
}

func TestNormalizeStartCodePreservesProviderTimeout(t *testing.T) {
	if got := normalizeStartCode(errorProviderTimeout); got != errorProviderTimeout {
		t.Fatalf("provider timeout normalized to %q", got)
	}
}

func TestOversizedSteerMapsToInputTooLarge(t *testing.T) {
	sys := &baseTestSys{}
	owner := testItem("owner", "chat", `{"text":"x"}`)
	steer := testItem("steer", TypeSteer, `{"text":"too large"}`)
	op := OpID("op-1")
	turn := &baseTurn{seq: 1, turnID: "turn", owner: owner, scope: owner.scope, ops: map[OpID]*turnOp{op: {kind: turnOpSteer, blocking: true}}}
	l := &agentLoop{sys: sys, rt: &baseTestRuntime{}, book: baseBook{turn: turn, committing: map[OpID]*commitOp{op: {kind: commitSteer, items: []*requestItem{steer}, targetTurn: 1}}}}
	l.controlDone(runtimeEvent{kind: evControlDone, op: op, turnID: "turn", verdict: ControlInputTooLarge, detail: "input byte limit exceeded"})
	if len(sys.fails) != 1 || sys.fails[0] != errorInputTooLarge {
		t.Fatalf("steer failures=%v", sys.fails)
	}
}
