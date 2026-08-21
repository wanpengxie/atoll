package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type v7Terminal struct {
	fail  bool
	code  string
	value any
}

type v7Progress struct {
	status string
	value  any
}

type v7Timer struct {
	id      schedule.TimerID
	d       time.Duration
	typ     string
	payload any
}

type v7Resource struct{}

func (v7Resource) Create(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (v7Resource) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (v7Resource) Write(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (v7Resource) Delete(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (v7Resource) Stat(resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}
func (v7Resource) List(accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (v7Resource) Open(resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (v7Resource) CreateFile(resource.ResourceID, bool) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (v7Resource) CreateFileDecided(resource.ResourceID, bool) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

type v7Sys struct {
	actorbase.Sys
	mu        sync.Mutex
	terminals map[string][]v7Terminal
	progress  map[string][]v7Progress
	timers    []v7Timer
	cancelled []schedule.TimerID
	afterErr  bool
}

func newV7Sys() *v7Sys {
	return &v7Sys{terminals: map[string][]v7Terminal{}, progress: map[string][]v7Progress{}}
}
func (*v7Sys) Self() actor.ActorID { return "agent:test:1" }
func (s *v7Sys) Reply(msg actorbase.Msg, value any) (message.ID, error) {
	s.mu.Lock()
	s.terminals[string(msg.ID)] = append(s.terminals[string(msg.ID)], v7Terminal{value: value})
	s.mu.Unlock()
	return "reply", nil
}
func (s *v7Sys) Fail(msg actorbase.Msg, code, _ string) (message.ID, error) {
	s.mu.Lock()
	s.terminals[string(msg.ID)] = append(s.terminals[string(msg.ID)], v7Terminal{fail: true, code: code})
	s.mu.Unlock()
	return "fail", nil
}
func (s *v7Sys) Progress(msg actorbase.Msg, status string, value any) (message.ID, error) {
	s.mu.Lock()
	s.progress[string(msg.ID)] = append(s.progress[string(msg.ID)], v7Progress{status: status, value: value})
	s.mu.Unlock()
	return "progress", nil
}
func (*v7Sys) Emit(behavior.EventSpec) (message.ID, error) { return "event", nil }
func (s *v7Sys) After(d time.Duration, typ string, payload any, _ schedule.TimerHome) (schedule.TimerID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.afterErr {
		return "", errors.New("timer unavailable")
	}
	id := schedule.TimerID(fmt.Sprintf("timer-%d", len(s.timers)+1))
	s.timers = append(s.timers, v7Timer{id: id, d: d, typ: typ, payload: payload})
	return id, nil
}
func (s *v7Sys) CancelTimer(id schedule.TimerID) error {
	s.mu.Lock()
	s.cancelled = append(s.cancelled, id)
	s.mu.Unlock()
	return nil
}
func (*v7Sys) Resource() actorbase.ResourceHandle { return v7Resource{} }

func (s *v7Sys) terminal(id string) []v7Terminal {
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		got := append([]v7Terminal(nil), s.terminals[id]...)
		s.mu.Unlock()
		if len(got) > 0 || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Millisecond)
	}
}
func (s *v7Sys) progresses(id string) []v7Progress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]v7Progress(nil), s.progress[id]...)
}

func newV7Loop(t *testing.T, capabilities map[string]bool) (*agentLoop, *v7Sys, *captureRuntime) {
	t.Helper()
	sys := newV7Sys()
	rt := &captureRuntime{}
	vault := effectcap.NewVault()
	exec := newExecutor(sys, vault)
	exec.bindRuntime(rt)
	local, cancel := context.WithCancel(context.Background())
	controls := map[string]struct{}{TypeAsk: {}, TypeQueue: {}, TypeCompact: {}, TypeSelect: {}, TypeContext: {}, TypeSteer: {}, TypeInterrupt: {}, TypeHold: {}, TypeUnhold: {}, TypeReplace: {}}
	l := &agentLoop{
		def: definition{cfg: Config{Runtime: runtimeproto.Spec{Capabilities: capabilities}, RequestMaxCount: 16, BufferMaxCount: 16, BufferMaxBytes: 1 << 20, BatchMaxCount: 16, ReceiptDeadline: time.Hour}, controls: controls},
		sys: sys, rt: rt, vault: vault, exec: exec, state: book.New(), inbox: newLoopInbox(64), local: local, cancel: cancel,
		receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: testLogger(), nowFn: time.Now,
	}
	t.Cleanup(func() {
		cancel()
		for _, timer := range l.receiptTimers {
			timer.Stop()
		}
	})
	return l, sys, rt
}

func v7Request(id, typ, sender, payload string) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), Sender: message.Sender{ID: actor.ActorID(sender)}, Kind: message.KindRequest, Type: typ, Payload: json.RawMessage(`{"body":` + payload + `}`)})
}
func v7Event(id, typ, payload string) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{ID: message.ID(id), Sender: message.Sender{ID: "agent:test:1"}, Kind: message.KindEvent, Type: typ, Payload: json.RawMessage(payload)})
}
func v7Activate(t *testing.T, l *agentLoop, id string) {
	t.Helper()
	l.handleIntake(v7Request(id, TypeAsk, "caller", `{"text":"`+id+`"}`))
	if l.state.Turn == nil {
		t.Fatal("request did not enter Starting")
	}
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: runtimeproto.TurnID("turn-" + id)})
}

func TestAgentControl01ControlsAreImmediatelyTerminalAndLeaveNoRows(t *testing.T) {
	for _, typ := range []string{TypeHold, TypeUnhold, TypeInterrupt} {
		t.Run(typ, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, nil)
			l.handleIntake(v7Request("control", typ, "caller", `{}`))
			if l.state.Requests["control"] != nil || len(sys.terminal("control")) != 1 || len(sys.progresses("control")) != 0 {
				t.Fatalf("row=%v terminal=%v progress=%v", l.state.Requests["control"], sys.terminal("control"), sys.progresses("control"))
			}
		})
	}
}

func TestAgentControl02ReplacePreservesBufferedIndex(t *testing.T) {
	l, _, _ := newV7Loop(t, nil)
	l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
	l.handleIntake(v7Request("r2", TypeAsk, "caller", `{"text":"two"}`))
	l.handleIntake(v7Request("r3", TypeAsk, "caller", `{"text":"three"}`))
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"r2"}`))
	l.handleIntake(v7Request("r2-new", TypeReplace, "caller", `{"target":"r2","old_text":"two","new_text":"two-new"}`))
	if got := l.state.Buffer; len(got) != 2 || got[0] != "r2-new" || got[1] != "r3" {
		t.Fatalf("buffer=%v", got)
	}
}

func TestAgentControl03FirstPositionProgressMatchesResult(t *testing.T) {
	t.Run("pending means queued", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, nil)
		l.state.Pending = &book.Action{Kind: book.ActionCleanup}
		l.handleIntake(v7Request("queued", TypeAsk, "caller", `{"text":"wait"}`))
		got := sys.progresses("queued")
		if len(got) != 1 || got[0].status != message.StatusQueued {
			t.Fatalf("progress=%v", got)
		}
	})
	t.Run("idle means processing only", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, nil)
		l.handleIntake(v7Request("direct", TypeAsk, "caller", `{"text":"go"}`))
		got := sys.progresses("direct")
		if len(got) != 1 || got[0].status != message.StatusProcessing {
			t.Fatalf("progress=%v", got)
		}
	})
}

func TestAgentControl04ResumedRequestFormsSingleItemBatch(t *testing.T) {
	l, _, rt := newV7Loop(t, nil)
	for idx, id := range []book.RequestID{"r1", "r2", "r3"} {
		row := &book.Request{ID: id, Sender: "caller", Input: runtimeproto.Input{SourceID: string(id), Text: string(id)}, Bytes: 2, Location: book.Buffered, Resumed: idx == 0}
		row.Scope = l.vault.Mint(string(id), string(id))
		l.state.Requests[id] = row
		l.state.Buffer = append(l.state.Buffer, id)
		l.state.BufferBytes += row.Bytes
	}
	l.startNext()
	if len(rt.starts) != 1 || len(rt.starts[0].Messages) != 1 || rt.starts[0].Messages[0].SourceID != "r1" {
		t.Fatalf("starts=%+v", rt.starts)
	}
}

func TestAgentControl05FrozenGuardStopsStartNext(t *testing.T) {
	l, _, rt := newV7Loop(t, nil)
	l.freeze("h", time.Minute)
	row := &book.Request{ID: "r", Input: runtimeproto.Input{Text: "r"}, Location: book.Buffered}
	l.state.Requests[row.ID] = row
	l.state.Buffer = []book.RequestID{row.ID}
	l.startNext()
	if !l.frozen(l.now()) || len(rt.starts) != 0 {
		t.Fatalf("frozen=%v starts=%d", l.frozen(l.now()), len(rt.starts))
	}
}

func TestAgentControl06UnholdIsIdempotentAndRestartsQueue(t *testing.T) {
	l, sys, rt := newV7Loop(t, nil)
	l.freeze("h", time.Minute)
	row := &book.Request{ID: "r", Input: runtimeproto.Input{Text: "r"}, Location: book.Buffered, Scope: l.vault.Mint("r", "r")}
	l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
	l.handleIntake(v7Request("u1", TypeUnhold, "caller", `{}`))
	l.handleIntake(v7Request("u2", TypeUnhold, "caller", `{}`))
	if l.frozen(l.now()) || len(rt.starts) != 1 || len(sys.terminal("u1")) != 1 || len(sys.terminal("u2")) != 1 {
		t.Fatalf("frozen=%v starts=%d", l.frozen(l.now()), len(rt.starts))
	}
}

func TestAgentControl07LaterHoldDetachesEarlierPendingInterrupt(t *testing.T) {
	l, _, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	owner := &book.Request{ID: "owner", Sender: "caller", Location: book.Starting}
	l.state.Requests[owner.ID] = owner
	l.state.Turn = &book.Turn{Phase: book.TurnStarting, Owner: owner.ID}
	l.handleIntake(v7Request("h1", TypeHold, "caller", `{"target":"owner","duration_ms":1000}`))
	if l.state.Pending == nil {
		t.Fatal("first hold did not queue interrupt")
	}
	l.handleIntake(v7Request("h2", TypeHold, "caller", `{"duration_ms":2000}`))
	if l.state.Pending != nil || l.heldBy != "h2" {
		t.Fatalf("pending=%+v heldBy=%q", l.state.Pending, l.heldBy)
	}
	l.handleHoldExpired(json.RawMessage(`{"hold_id":"h1"}`))
	if l.heldBy != "h2" {
		t.Fatal("late fire cleared newer hold")
	}
}

func TestAgentControl08SuccessfulSteerDoesNotClearFreeze(t *testing.T) {
	l, _, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("h", TypeHold, "caller", `{}`))
	l.handleIntake(v7Request("s", TypeSteer, "caller", `{"text":"new direction","expected_turn_id":"turn-owner"}`))
	if !l.frozen(l.now()) {
		t.Fatal("steer cleared freeze")
	}
}

func TestAgentControl09DegradedSteerClearsFreeze(t *testing.T) {
	l, _, _ := newV7Loop(t, nil)
	l.handleIntake(v7Request("h", TypeHold, "caller", `{}`))
	l.handleIntake(v7Request("s", TypeSteer, "caller", `{"text":"later"}`))
	if l.frozen(l.now()) {
		t.Fatal("queued steer left queue frozen")
	}
}

func TestAgentControl10RebufferDoesNotClearItsOwnFreeze(t *testing.T) {
	l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
	row := l.state.Requests["owner"]
	progress := sys.progresses("owner")
	if !l.frozen(l.now()) || row == nil || !row.Resumed || row.Location != book.Buffered || len(progress) < 3 {
		t.Fatalf("frozen=%v row=%+v progress=%v", l.frozen(l.now()), row, progress)
	}
}

func TestAgentControl11CancelBufferedRequestDoesNotUnfreeze(t *testing.T) {
	l, _, _ := newV7Loop(t, nil)
	l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
	l.handleIntake(v7Request("r", TypeAsk, "caller", `{"text":"later"}`))
	l.handleIntake(v7Request("h", TypeHold, "caller", `{}`))
	l.handleClosure("r")
	if !l.frozen(l.now()) || l.state.Turn == nil {
		t.Fatal("cancel changed freeze or turn")
	}
}

func TestAgentControl12ExpiryUsesClockAndAllFireBranches(t *testing.T) {
	l, sys, rt := newV7Loop(t, nil)
	now := time.Unix(100, 0)
	l.nowFn = func() time.Time { return now }
	l.freeze("h", time.Second)
	l.handleHoldExpired(json.RawMessage(`{"hold_id":"old"}`))
	if l.heldBy != "h" {
		t.Fatal("mismatched fire changed hold")
	}
	l.handleHoldExpired(json.RawMessage(`{"hold_id":"h"}`))
	if l.heldBy != "h" || len(sys.timers) != 2 {
		t.Fatalf("early fire heldBy=%q timers=%d", l.heldBy, len(sys.timers))
	}
	now = now.Add(2 * time.Second)
	l.handleHoldExpired(json.RawMessage(`{"hold_id":"h"}`))
	if l.heldBy != "" {
		t.Fatal("expired matching fire did not clear")
	}
	l.freeze("h2", time.Second)
	now = now.Add(2 * time.Second)
	l.handleIntake(v7Request("ask", TypeAsk, "caller", `{"text":"go"}`))
	if l.frozen(now) || len(rt.starts) != 1 {
		t.Fatal("clock fallback did not advance ask")
	}
	sys.afterErr = true
	l.freeze("h3", time.Second)
	if !l.frozen(now) {
		t.Fatal("After failure prevented freeze")
	}
}

func TestAgentControl13CapacityFailureStillUnfreezesAndAdvancesExistingQueue(t *testing.T) {
	l, sys, rt := newV7Loop(t, nil)
	l.def.cfg.BufferMaxCount = 1
	row := &book.Request{ID: "stock", Input: runtimeproto.Input{Text: "stock"}, Bytes: 5, Location: book.Buffered, Scope: l.vault.Mint("stock", "stock")}
	l.state.Requests[row.ID], l.state.Buffer, l.state.BufferBytes = row, []book.RequestID{row.ID}, row.Bytes
	l.freeze("h", time.Minute)
	l.handleIntake(v7Request("overflow", TypeAsk, "caller", `{"text":"overflow"}`))
	if l.frozen(l.now()) || len(rt.starts) != 1 || sys.terminal("overflow")[0].code != errorBaseCapacity {
		t.Fatalf("frozen=%v starts=%d terminal=%v", l.frozen(l.now()), len(rt.starts), sys.terminal("overflow"))
	}
}

func TestAgentControl14RequestCapacityGateExemptsControlAndReplace(t *testing.T) {
	for _, typ := range []string{TypeHold, TypeUnhold, TypeInterrupt} {
		l, sys, _ := newV7Loop(t, nil)
		l.def.cfg.RequestMaxCount = 1
		l.state.Requests["full"] = &book.Request{ID: "full"}
		l.handleIntake(v7Request("control", typ, "caller", `{}`))
		if got := sys.terminal("control"); len(got) != 1 || got[0].fail {
			t.Fatalf("%s terminal=%v", typ, got)
		}
	}
	l, sys, _ := newV7Loop(t, nil)
	l.def.cfg.RequestMaxCount = 1
	target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered, Scope: l.vault.Mint("target", "target")}
	l.state.Requests[target.ID], l.state.Buffer, l.state.BufferBytes = target, []book.RequestID{target.ID}, target.Bytes
	l.handleIntake(v7Request("replacement", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new"}`))
	if l.state.Requests["replacement"] == nil || len(sys.progresses("replacement")) != 1 {
		t.Fatalf("replace hit request table gate: row=%v progress=%v", l.state.Requests["replacement"], sys.progresses("replacement"))
	}
	l2, sys2, _ := newV7Loop(t, nil)
	l2.def.cfg.RequestMaxCount = 1
	l2.state.Requests["full"] = &book.Request{ID: "full"}
	l2.handleIntake(v7Request("ask", TypeAsk, "caller", `{"text":"no"}`))
	if sys2.terminal("ask")[0].code != errorBaseCapacity {
		t.Fatalf("ask terminal=%v", sys2.terminal("ask"))
	}
}

func TestAgentControl15HoldAndInterruptSettleOwnersDifferently(t *testing.T) {
	t.Run("hold requeues owner", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
		v7Activate(t, l, "owner")
		l.handleIntake(v7Request("hold", TypeHold, "caller", `{"target":"owner"}`))
		l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
		row := l.state.Requests["owner"]
		last := sys.progresses("owner")[2]
		value := last.value.(map[string]any)
		if row == nil || !row.Resumed || value["held_by"] != book.RequestID("hold") || len(sys.terminal("owner")) != 0 {
			t.Fatalf("row=%+v progress=%v terminal=%v", row, last, sys.terminal("owner"))
		}
	})
	t.Run("interrupt fails owner", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
		v7Activate(t, l, "owner")
		l.handleIntake(v7Request("interrupt", TypeInterrupt, "caller", `{}`))
		l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
		got := sys.terminal("owner")
		if len(got) != 1 || got[0].code != errorInterrupted || l.state.Requests["owner"] != nil {
			t.Fatalf("terminal=%v row=%v", got, l.state.Requests["owner"])
		}
	})
}

func TestAgentControl16MatchingRunningInterruptWinsOverLaterHold(t *testing.T) {
	l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
	got := sys.terminal("owner")
	if len(got) != 1 || got[0].code != errorInterrupted || l.state.Requests["owner"] != nil {
		t.Fatalf("terminal=%v row=%v", got, l.state.Requests["owner"])
	}
}

func TestAgentControl17RunningHoldKeepsDispositionAfterUnfreeze(t *testing.T) {
	l, _, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	l.handleIntake(v7Request("new", TypeAsk, "caller", `{"text":"also do this"}`))
	if l.frozen(l.now()) {
		t.Fatal("new message did not unfreeze")
	}
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
	if row := l.state.Requests["owner"]; row == nil || !row.Resumed || row.Location != book.Buffered {
		t.Fatalf("owner=%+v", row)
	}
}

func TestAgentControl18PendingHoldInterruptIsDetachedOnUnfreeze(t *testing.T) {
	l, _, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	l.handleIntake(v7Request("owner", TypeAsk, "caller", `{"text":"owner"}`))
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	if l.state.Pending == nil {
		t.Fatal("hold interrupt not pending")
	}
	l.handleIntake(v7Request("new", TypeAsk, "caller", `{"text":"new"}`))
	if l.state.Pending != nil {
		t.Fatal("hold interrupt survived unfreeze")
	}
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn"})
	if len(rt.controls) != 0 {
		t.Fatalf("controls=%v", rt.controls)
	}
}

func TestAgentControl19PendingInterruptSurvivesUnfreeze(t *testing.T) {
	l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	l.handleIntake(v7Request("owner", TypeAsk, "caller", `{"text":"owner"}`))
	l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
	l.handleIntake(v7Request("new", TypeAsk, "caller", `{"text":"new"}`))
	if l.state.Pending == nil || l.state.Pending.Disposition != book.DispFailOwner {
		t.Fatalf("pending=%+v", l.state.Pending)
	}
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn"})
	if len(rt.controls) != 1 {
		t.Fatalf("controls=%v", rt.controls)
	}
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn", status: runtimeproto.TurnStatusInterrupted})
	if got := sys.terminal("owner"); len(got) != 1 || got[0].code != errorInterrupted {
		t.Fatalf("owner terminal=%v", got)
	}
}

func TestAgentControl20ControlAndTurnArrivalOrdersHaveSingleTerminals(t *testing.T) {
	orders := []string{"control-first", "turn-first", "provider-lost-first"}
	for _, order := range orders {
		t.Run(order, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
			v7Activate(t, l, "owner")
			l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
			a := l.state.Running
			control := runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted}
			ended := runtimeEvent{kind: evTurnEnded, turnID: a.Target, status: runtimeproto.TurnStatusInterrupted}
			lost := runtimeEvent{kind: evProviderLost, turnID: a.Target, cause: runtimeproto.LostCrash}
			switch order {
			case "control-first":
				l.onControlDone(control)
				l.onTurnEnded(ended)
				l.onProviderLost(lost)
			case "turn-first":
				l.onTurnEnded(ended)
				l.onControlDone(control)
				l.onProviderLost(lost)
			case "provider-lost-first":
				l.onProviderLost(lost)
				l.onControlDone(control)
				l.onTurnEnded(ended)
			}
			if owner, interrupt := sys.terminal("owner"), sys.terminal("i"); len(owner) != 1 || len(interrupt) != 1 || l.state.Requests["i"] != nil {
				t.Fatalf("owner=%v interrupt=%v row=%v", owner, interrupt, l.state.Requests["i"])
			}
		})
	}
}

func TestAgentControl21RebufferMintsScopeThatReallyAdmitsResourceEffect(t *testing.T) {
	l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	oldScope := l.state.Requests["owner"].Scope
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-owner", status: runtimeproto.TurnStatusInterrupted})
	newScope := l.state.Requests["owner"].Scope
	a := l.state.Running
	l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted})
	l.clearFreeze()
	l.startNext()
	if l.state.Turn == nil || l.state.Turn.Scope != newScope {
		t.Fatal("resumed turn did not use reminted scope")
	}
	bridge := newResourceBridge(sys, l.vault)
	if got := bridge.Invoke(context.Background(), l.state.Turn.Scope, runtimeproto.ResourceInvocation{Operation: "stat", ResourceID: "resource"}); got.Error != "" {
		t.Fatalf("new scope resource effect failed: %+v", got)
	}
	if got := bridge.Invoke(context.Background(), oldScope, runtimeproto.ResourceInvocation{Operation: "stat", ResourceID: "resource"}); got.Error != "effect scope is not open" {
		t.Fatalf("old scope effect=%+v", got)
	}
}

func TestAgentControl22HoldTargetAdmissionIsStrict(t *testing.T) {
	tests := []struct {
		name, sender string
		row          *book.Request
		code         string
	}{
		{name: "control pending", sender: "caller", row: &book.Request{ID: "target", Sender: "caller", Location: book.ControlPending}, code: errorCASMismatch},
		{name: "missing", sender: "caller", code: errorCASMismatch},
		{name: "other sender", sender: "other", row: &book.Request{ID: "target", Sender: "caller", Location: book.Buffered}, code: "target_not_owned"},
		{name: "command", sender: "caller", row: &book.Request{ID: "target", Sender: "caller", Location: book.Buffered, TurnKind: runtimeproto.TurnCompact}, code: "invalid_args"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, nil)
			if test.row != nil {
				l.state.Requests[test.row.ID] = test.row
				if test.row.Location == book.Buffered {
					l.state.Buffer = []book.RequestID{test.row.ID}
				}
			}
			l.handleIntake(v7Request("h", TypeHold, test.sender, `{"target":"target"}`))
			if got := sys.terminal("h"); len(got) != 1 || got[0].code != test.code || l.frozen(l.now()) {
				t.Fatalf("terminal=%v frozen=%v", got, l.frozen(l.now()))
			}
		})
	}
}

func TestAgentControl23DurationMillisecondsDomainIsClosed(t *testing.T) {
	invalid := []string{`0`, `-1`, `1.5`, `null`, `1800001`, `999999999999999999999999`}
	for _, raw := range invalid {
		l, sys, _ := newV7Loop(t, nil)
		l.handleIntake(v7Request("h", TypeHold, "caller", `{"duration_ms":`+raw+`}`))
		if got := sys.terminal("h"); len(got) != 1 || got[0].code != "invalid_args" || l.frozen(l.now()) {
			t.Fatalf("duration=%s terminal=%v frozen=%v", raw, got, l.frozen(l.now()))
		}
	}
	for _, payload := range []string{`{}`, `{"duration_ms":1800000}`} {
		l, sys, _ := newV7Loop(t, nil)
		l.handleIntake(v7Request("h", TypeHold, "caller", payload))
		if got := sys.terminal("h"); len(got) != 1 || got[0].fail || !l.frozen(l.now()) {
			t.Fatalf("payload=%s terminal=%v", payload, got)
		}
	}
}

func TestAgentControl24InterruptWithoutCapabilityStillFreezesAndCompletes(t *testing.T) {
	l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: false})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
	got := sys.terminal("i")
	if len(got) != 1 || got[0].fail || !l.frozen(l.now()) || len(rt.controls) != 0 || l.state.Turn == nil {
		t.Fatalf("terminal=%v frozen=%v controls=%v turn=%v", got, l.frozen(l.now()), rt.controls, l.state.Turn)
	}
}

func TestAgentControl25ReplaceFailuresAreAtomicAndSuccessNeedsNoFreeze(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
	target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered, Scope: l.vault.Mint("target", "target")}
	l.state.Requests[target.ID], l.state.Buffer, l.state.BufferBytes = target, []book.RequestID{target.ID}, target.Bytes
	l.freeze("h", time.Minute)
	beforeUntil := l.frozenUntil
	l.handleIntake(v7Request("bad", TypeReplace, "caller", `{"target":"target","old_text":"wrong","new_text":"new"}`))
	if got := sys.terminal("bad"); len(got) != 1 || got[0].code != errorCASMismatch || l.state.Buffer[0] != "target" || l.state.BufferBytes != 3 || l.frozenUntil != beforeUntil {
		t.Fatalf("terminal=%v state=%+v", got, l.state)
	}
	l.clearFreeze()
	l.handleIntake(v7Request("good", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new"}`))
	if l.state.Requests["target"] != nil || l.state.Requests["good"] == nil || len(sys.progresses("good")) != 1 {
		t.Fatalf("requests=%v progress=%v", l.state.Requests, sys.progresses("good"))
	}

	l2, sys2, _ := newV7Loop(t, nil)
	l2.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
	l2.def.cfg.BufferMaxBytes = 3
	t2 := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered}
	l2.state.Requests[t2.ID], l2.state.Buffer, l2.state.BufferBytes = t2, []book.RequestID{t2.ID}, 3
	l2.handleIntake(v7Request("large", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"much larger"}`))
	if got := sys2.terminal("large"); len(got) != 1 || got[0].code != errorBaseCapacity || l2.state.Buffer[0] != "target" || l2.state.BufferBytes != 3 {
		t.Fatalf("terminal=%v state=%+v", got, l2.state)
	}
}

func TestAgentControl26ReplaceBuildsProviderInputAndResumedTemplate(t *testing.T) {
	t.Run("plain text and attachments", func(t *testing.T) {
		l, _, rt := newV7Loop(t, nil)
		target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered, Scope: l.vault.Mint("target", "target")}
		l.state.Requests[target.ID], l.state.Buffer, l.state.BufferBytes = target, []book.RequestID{target.ID}, 3
		l.handleIntake(v7Request("new", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new text","attachments":[{"address":"daemon://file","name":"f"}]}`))
		input := rt.starts[0].Messages[0]
		if input.Text != "new text" || len(input.Attachments) != 1 || input.Attachments[0].Address != "daemon://file" {
			t.Fatalf("input=%+v", input)
		}
	})
	t.Run("resumed correction template", func(t *testing.T) {
		l, _, rt := newV7Loop(t, nil)
		target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered, Resumed: true, Scope: l.vault.Mint("target", "target")}
		l.state.Requests[target.ID], l.state.Buffer, l.state.BufferBytes = target, []book.RequestID{target.ID}, 3
		l.handleIntake(v7Request("new", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new"}`))
		want := `用户明确将 "old" 修改为 "new"，请遵循更新之后的指令或信息，其余保持不变。`
		if got := rt.starts[0].Messages[0].Text; got != want {
			t.Fatalf("text=%q want=%q", got, want)
		}
	})
}

func TestAgentControl27MergedOwnerReplaceKeepsChainAndRunsCorrectionAlone(t *testing.T) {
	l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	l.state.Pending = &book.Action{Kind: book.ActionCleanup}
	l.handleIntake(v7Request("r2", TypeAsk, "caller", `{"text":"two"}`))
	l.handleIntake(v7Request("r3", TypeAsk, "caller", `{"text":"three"}`))
	l.state.Pending = nil
	l.startNext()
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn"})
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"r3"}`))
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn", status: runtimeproto.TurnStatusInterrupted})
	a := l.state.Running
	l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted})
	l.handleIntake(v7Request("r3-new", TypeReplace, "caller", `{"target":"r3","old_text":"three","new_text":"three-new"}`))
	if got := sys.terminal("r2"); len(got) != 1 || got[0].fail {
		t.Fatalf("r2 terminal=%v", got)
	}
	if len(rt.starts) != 2 || len(rt.starts[1].Messages) != 1 || rt.starts[1].Messages[0].SourceID != "r3-new" || rt.starts[1].Messages[0].Text == "three-new" {
		t.Fatalf("starts=%+v", rt.starts)
	}
}

func TestAgentControl28ManifestSchemasAndErrorsValidateWithoutReservedCodes(t *testing.T) {
	manifest := Manifest("agent", map[string]bool{})
	if err := introspect.ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{TypeHold, TypeUnhold, TypeReplace, TypeInterrupt} {
		spec := manifest.Words[typ]
		if len(spec.InputSchema) == 0 {
			t.Fatalf("%s missing schema", typ)
		}
		for _, code := range spec.ErrorCodes {
			if code == "forbidden" || code == "busy" {
				t.Fatalf("%s contains reserved/dead code %s", typ, code)
			}
		}
	}
}

func TestAgentControl29ContextIncludesOnlyLiveFrozenState(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	l.freeze("h", time.Minute)
	l.handleIntake(v7Request("c1", TypeContext, "caller", `{}`))
	value := sys.terminal("c1")[0].value.(map[string]any)
	frozen := value["frozen"].(map[string]any)
	if frozen["held_by"] != book.RequestID("h") || frozen["until"] == nil {
		t.Fatalf("context=%v", value)
	}
	l.clearFreeze()
	l.handleIntake(v7Request("c2", TypeContext, "caller", `{}`))
	if value := sys.terminal("c2")[0].value.(map[string]any); value["frozen"] != nil {
		t.Fatalf("context=%v", value)
	}
}
