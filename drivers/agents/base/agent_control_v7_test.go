package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

func TestAgentControl07LaterHoldResetsHolderAndDeadline(t *testing.T) {
	l, _, _ := newV7Loop(t, nil)
	now := time.Unix(100, 0)
	l.nowFn = func() time.Time { return now }
	l.handleIntake(v7Request("h1", TypeHold, "caller", `{"duration_ms":1000}`))
	if l.heldBy != "h1" || !l.frozenUntil.Equal(now.Add(time.Second)) {
		t.Fatalf("first hold heldBy=%q until=%v", l.heldBy, l.frozenUntil)
	}
	now = now.Add(250 * time.Millisecond)
	l.handleIntake(v7Request("h2", TypeHold, "caller", `{"duration_ms":2000}`))
	wantUntil := now.Add(2 * time.Second)
	if l.heldBy != "h2" || !l.frozenUntil.Equal(wantUntil) {
		t.Fatalf("replacement hold heldBy=%q until=%v want=%v", l.heldBy, l.frozenUntil, wantUntil)
	}
	l.handleHoldExpired(json.RawMessage(`{"hold_id":"h1"}`))
	if l.heldBy != "h2" || !l.frozenUntil.Equal(wantUntil) {
		t.Fatal("late fire changed the newer hold")
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
	t.Run("clock expiry advances without another message", func(t *testing.T) {
		l, _, rt := newV7Loop(t, nil)
		now := time.Unix(100, 0)
		l.nowFn = func() time.Time { return now }
		row := &book.Request{ID: "queued", Input: runtimeproto.Input{Text: "queued"}, Location: book.Buffered, Scope: l.vault.Mint("queued", "queued")}
		l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
		l.freeze("h", time.Second)
		now = now.Add(2 * time.Second)
		l.startNext()
		if len(rt.starts) != 1 || l.state.Turn == nil {
			t.Fatalf("starts=%d turn=%+v", len(rt.starts), l.state.Turn)
		}
	})
	t.Run("fire mismatch early and expired", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, nil)
		now := time.Unix(100, 0)
		l.nowFn = func() time.Time { return now }
		row := &book.Request{ID: "queued", Input: runtimeproto.Input{Text: "queued"}, Location: book.Buffered, Scope: l.vault.Mint("queued", "queued")}
		l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
		l.freeze("h", time.Second)
		l.handleHoldExpired(json.RawMessage(`{"hold_id":"old"}`))
		if l.heldBy != "h" || len(rt.starts) != 0 {
			t.Fatal("mismatched fire changed hold")
		}
		l.handleHoldExpired(json.RawMessage(`{"hold_id":"h"}`))
		if l.heldBy != "h" || len(sys.timers) != 2 || sys.timers[1].d != time.Second {
			t.Fatalf("early fire heldBy=%q timers=%+v", l.heldBy, sys.timers)
		}
		now = now.Add(2 * time.Second)
		l.handleHoldExpired(json.RawMessage(`{"hold_id":"h"}`))
		if l.heldBy != "" || len(rt.starts) != 1 {
			t.Fatalf("expired fire heldBy=%q starts=%d", l.heldBy, len(rt.starts))
		}
	})
	t.Run("After failure still expires by clock", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, nil)
		now := time.Unix(100, 0)
		l.nowFn = func() time.Time { return now }
		sys.afterErr = true
		row := &book.Request{ID: "queued", Input: runtimeproto.Input{Text: "queued"}, Location: book.Buffered, Scope: l.vault.Mint("queued", "queued")}
		l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
		l.freeze("h", time.Second)
		if !l.frozen(now) || len(sys.timers) != 0 {
			t.Fatal("After failure prevented freeze")
		}
		now = now.Add(2 * time.Second)
		l.startNext()
		if l.frozen(now) || len(rt.starts) != 1 {
			t.Fatalf("frozen=%v starts=%d", l.frozen(now), len(rt.starts))
		}
	})
	t.Run("late fire after CancelTimer is a no-op", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, nil)
		l.freeze("h", time.Second)
		l.clearFreeze()
		if len(sys.cancelled) != 1 {
			t.Fatalf("cancelled=%v", sys.cancelled)
		}
		l.handleHoldExpired(json.RawMessage(`{"hold_id":"h"}`))
		if l.heldBy != "" || !l.frozenUntil.IsZero() || len(rt.starts) != 0 {
			t.Fatalf("heldBy=%q until=%v starts=%d", l.heldBy, l.frozenUntil, len(rt.starts))
		}
	})
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

func TestAgentControl16RunningInterruptRejectsEveryNewInterrupt(t *testing.T) {
	l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
	until := l.frozenUntil
	l.handleIntake(v7Request("h", TypeHold, "caller", `{"target":"owner"}`))
	l.handleIntake(v7Request("i2", TypeInterrupt, "caller", `{}`))
	for _, id := range []string{"h", "i2"} {
		if got := sys.terminal(id); len(got) != 1 || got[0].code != "busy" {
			t.Fatalf("%s terminal=%v", id, got)
		}
	}
	if l.heldBy != "i" || l.frozenUntil != until || l.state.Pending != nil || l.state.Running == nil || l.state.Running.Kind != book.ActionInterrupt {
		t.Fatalf("heldBy=%q until=%v pending=%+v running=%+v", l.heldBy, l.frozenUntil, l.state.Pending, l.state.Running)
	}
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

func TestAgentControl18StartingWindowRejectsInterruptsWithoutEffects(t *testing.T) {
	for _, typ := range []string{TypeInterrupt, TypeHold} {
		t.Run(typ, func(t *testing.T) {
			l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
			l.handleIntake(v7Request("owner", TypeAsk, "caller", `{"text":"owner"}`))
			payload := `{}`
			if typ == TypeHold {
				payload = `{"target":"owner"}`
			}
			l.handleIntake(v7Request("control", typ, "caller", payload))
			if got := sys.terminal("control"); len(got) != 1 || got[0].code != "busy" {
				t.Fatalf("terminal=%v", got)
			}
			if l.frozen(l.now()) || l.heldBy != "" || l.state.Pending != nil || l.state.Running != nil || len(rt.controls) != 0 {
				t.Fatalf("frozen=%v heldBy=%q pending=%+v running=%+v controls=%v", l.frozen(l.now()), l.heldBy, l.state.Pending, l.state.Running, rt.controls)
			}
			l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn"})
			l.handleIntake(v7Request("retry", typ, "caller", payload))
			if got := sys.terminal("retry"); len(got) != 1 || got[0].fail || !l.frozen(l.now()) || len(rt.controls) != 1 {
				t.Fatalf("retry=%v frozen=%v controls=%v", got, l.frozen(l.now()), rt.controls)
			}
		})
	}
}

func TestAgentControl19PendingSlotNeverContainsInterrupt(t *testing.T) {
	assertPending := func(t *testing.T, l *agentLoop) {
		t.Helper()
		if l.state.Pending != nil && l.state.Pending.Kind == book.ActionInterrupt {
			t.Fatalf("interrupt entered Pending: %+v", l.state.Pending)
		}
	}
	l, _, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true, runtimeproto.CapabilitySteer: true})
	l.handleIntake(v7Request("owner", TypeAsk, "caller", `{"text":"owner"}`))
	l.handleIntake(v7Request("i-starting", TypeInterrupt, "caller", `{}`))
	assertPending(t, l)
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn"})
	l.handleIntake(v7Request("i-running", TypeInterrupt, "caller", `{}`))
	assertPending(t, l)
	l.handleIntake(v7Request("i-busy", TypeInterrupt, "caller", `{}`))
	assertPending(t, l)
	l.clearFreeze()
	assertPending(t, l)
}

func TestAgentControl20ControlAndTurnArrivalOrdersHaveSingleTerminals(t *testing.T) {
	orders := []struct {
		name  string
		steps []string
		code  string
	}{
		{name: "control-turn-lost", steps: []string{"control", "turn", "lost"}, code: errorInterrupted},
		{name: "control-lost-turn", steps: []string{"control", "lost", "turn"}, code: errorProviderCrash},
		{name: "turn-control-lost", steps: []string{"turn", "control", "lost"}, code: errorInterrupted},
		{name: "turn-lost-control", steps: []string{"turn", "lost", "control"}, code: errorInterrupted},
		{name: "lost-control-turn", steps: []string{"lost", "control", "turn"}, code: errorProviderCrash},
		{name: "lost-turn-control", steps: []string{"lost", "turn", "control"}, code: errorProviderCrash},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilityInterrupt: true})
			v7Activate(t, l, "owner")
			l.handleIntake(v7Request("i", TypeInterrupt, "caller", `{}`))
			a := l.state.Running
			control := runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted}
			ended := runtimeEvent{kind: evTurnEnded, turnID: a.Target, status: runtimeproto.TurnStatusInterrupted}
			lost := runtimeEvent{kind: evProviderLost, turnID: a.Target, cause: runtimeproto.LostCrash}
			for _, step := range order.steps {
				switch step {
				case "control":
					l.onControlDone(control)
				case "turn":
					l.onTurnEnded(ended)
				case "lost":
					l.onProviderLost(lost)
				}
			}
			owner, interrupt := sys.terminal("owner"), sys.terminal("i")
			if len(owner) != 1 || owner[0].code != order.code || len(interrupt) != 1 || interrupt[0].fail || l.state.Requests["i"] != nil {
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
		now := time.Unix(100, 0)
		l.nowFn = func() time.Time { return now }
		l.handleIntake(v7Request("h", TypeHold, "caller", payload))
		if got := sys.terminal("h"); len(got) != 1 || got[0].fail || !l.frozen(l.now()) || !l.frozenUntil.Equal(now.Add(30*time.Minute)) {
			t.Fatalf("payload=%s terminal=%v until=%v", payload, got, l.frozenUntil)
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
	tests := []struct {
		name, sender, payload, code string
		location                    book.Location
		maxBytes                    int
	}{
		{name: "format", sender: "caller", payload: `{"target":"target","old_text":"old"}`, code: "invalid_args", location: book.Buffered},
		{name: "attachment format", sender: "caller", payload: `{"target":"target","old_text":"old","new_text":"new","attachments":{}}`, code: "invalid_args", location: book.Buffered},
		{name: "missing target", sender: "caller", payload: `{"target":"missing","old_text":"old","new_text":"new"}`, code: errorCASMismatch, location: book.Buffered},
		{name: "ownership", sender: "other", payload: `{"target":"target","old_text":"old","new_text":"new"}`, code: "target_not_owned", location: book.Buffered},
		{name: "location", sender: "caller", payload: `{"target":"target","old_text":"old","new_text":"new"}`, code: errorCASMismatch, location: book.Workspace},
		{name: "old text", sender: "caller", payload: `{"target":"target","old_text":"wrong","new_text":"new"}`, code: errorCASMismatch, location: book.Buffered},
		{name: "capacity", sender: "caller", payload: `{"target":"target","old_text":"old","new_text":"much larger"}`, code: errorBaseCapacity, location: book.Buffered, maxBytes: 14},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, nil)
			l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
			if test.maxBytes > 0 {
				l.def.cfg.BufferMaxBytes = test.maxBytes
			}
			before := &book.Request{ID: "before", Sender: "caller", Input: runtimeproto.Input{Text: "before"}, Bytes: 6, Location: book.Buffered}
			target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: test.location, Scope: l.vault.Mint("target", "target")}
			after := &book.Request{ID: "after", Sender: "caller", Input: runtimeproto.Input{Text: "after"}, Bytes: 5, Location: book.Buffered}
			l.state.Requests[before.ID], l.state.Requests[target.ID], l.state.Requests[after.ID] = before, target, after
			l.state.Buffer = []book.RequestID{before.ID, target.ID, after.ID}
			l.state.BufferBytes = before.Bytes + target.Bytes + after.Bytes
			l.freeze("h", time.Minute)
			beforeUntil := l.frozenUntil
			l.handleIntake(v7Request("replacement", TypeReplace, test.sender, test.payload))
			got := sys.terminal("replacement")
			if len(got) != 1 || got[0].code != test.code || l.state.Requests["target"] != target || l.state.Requests["replacement"] != nil ||
				fmt.Sprint(l.state.Buffer) != "[before target after]" || l.state.IndexInBuffer("target") != 1 || l.state.BufferBytes != 14 || l.heldBy != "h" || l.frozenUntil != beforeUntil {
				t.Fatalf("terminal=%v requests=%v buffer=%v bytes=%d heldBy=%q until=%v", got, l.state.Requests, l.state.Buffer, l.state.BufferBytes, l.heldBy, l.frozenUntil)
			}
		})
	}

	l, sys, _ := newV7Loop(t, nil)
	l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
	target := &book.Request{ID: "target", Sender: "caller", Input: runtimeproto.Input{Text: "old"}, Bytes: 3, Location: book.Buffered, Scope: l.vault.Mint("target", "target")}
	after := &book.Request{ID: "after", Sender: "caller", Input: runtimeproto.Input{Text: "after"}, Bytes: 5, Location: book.Buffered}
	l.state.Requests[target.ID], l.state.Requests[after.ID] = target, after
	l.state.Buffer, l.state.BufferBytes = []book.RequestID{target.ID, after.ID}, target.Bytes+after.Bytes
	l.handleIntake(v7Request("good", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new"}`))
	if l.state.Requests["target"] != nil || l.state.Requests["good"] == nil || len(sys.progresses("good")) != 1 || l.state.IndexInBuffer("good") != 0 || l.state.Buffer[1] != "after" {
		t.Fatalf("requests=%v buffer=%v progress=%v", l.state.Requests, l.state.Buffer, sys.progresses("good"))
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
	l.handleIntake(v7Request("unhold", TypeUnhold, "caller", `{}`))
	r2 := sys.terminal("r2")
	r3 := sys.terminal("r3")
	if len(r2) != 1 || r2[0].fail || r2[0].value.(map[string]any)["merged_into"] != book.RequestID("r3") {
		t.Fatalf("r2 terminal=%v", r2)
	}
	if len(r3) != 1 || r3[0].fail || r3[0].value.(map[string]any)["replaced_by"] != book.RequestID("r3-new") {
		t.Fatalf("r3 terminal=%v", r3)
	}
	if len(rt.starts) != 2 || len(rt.starts[1].Messages) != 1 || rt.starts[1].Messages[0].SourceID != "r3-new" || rt.starts[1].Messages[0].Text == "three-new" {
		t.Fatalf("starts=%+v", rt.starts)
	}
}

func TestAgentControl28ManifestSchemasAndErrorsIncludeBusyButNoReservedCodes(t *testing.T) {
	manifest := Manifest("agent", map[string]bool{})
	if err := introspect.ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{TypeHold, TypeUnhold, TypeReplace, TypeInterrupt, TypeSteer} {
		spec := manifest.Words[typ]
		if len(spec.InputSchema) == 0 {
			t.Fatalf("%s missing schema", typ)
		}
		for _, code := range spec.ErrorCodes {
			if code == "forbidden" {
				t.Fatalf("%s contains reserved code %s", typ, code)
			}
		}
	}
	for _, typ := range []string{TypeHold, TypeInterrupt} {
		if !slices.Contains(manifest.Words[typ].ErrorCodes, "busy") {
			t.Fatalf("%s does not advertise busy: %v", typ, manifest.Words[typ].ErrorCodes)
		}
	}
	var steerSchema struct {
		OneOf []json.RawMessage `json:"oneOf"`
	}
	if err := json.Unmarshal(manifest.Words[TypeSteer].InputSchema, &steerSchema); err != nil || len(steerSchema.OneOf) != 3 {
		t.Fatalf("agent.steer schema=%s err=%v", manifest.Words[TypeSteer].InputSchema, err)
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

func TestAgentControl35SteerTargetAdmission(t *testing.T) {
	t.Run("target must be buffered", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		l.state.Requests["active"] = &book.Request{ID: "active", Sender: "caller", Location: book.Workspace}
		l.handleIntake(v7Request("insert", TypeSteer, "caller", `{"target":"active"}`))
		if got := sys.terminal("insert"); len(got) != 1 || got[0].code != errorCASMismatch {
			t.Fatalf("terminal=%v", got)
		}
	})
	t.Run("target must belong to sender", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		row := &book.Request{ID: "target", Sender: "owner", Location: book.Buffered}
		l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
		l.handleIntake(v7Request("insert", TypeSteer, "other", `{"target":"target"}`))
		if got := sys.terminal("insert"); len(got) != 1 || got[0].code != "target_not_owned" {
			t.Fatalf("terminal=%v", got)
		}
	})
	t.Run("target and text are mutually exclusive", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		l.handleIntake(v7Request("insert", TypeSteer, "caller", `{"target":"target","text":"also"}`))
		if got := sys.terminal("insert"); len(got) != 1 || got[0].code != "invalid_args" {
			t.Fatalf("terminal=%v", got)
		}
	})
	t.Run("target form bypasses request table capacity", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		row := &book.Request{ID: "target", Sender: "caller", Location: book.Buffered}
		l.state.Requests[row.ID], l.state.Buffer = row, []book.RequestID{row.ID}
		l.def.cfg.RequestMaxCount = 1
		l.handleIntake(v7Request("insert", TypeSteer, "caller", `{"target":"target"}`))
		if got := sys.terminal("insert"); len(got) != 1 || got[0].fail {
			t.Fatalf("terminal=%v", got)
		}
	})
}

func TestAgentControl36SteerTargetWithoutActiveTurnPromotesAndStarts(t *testing.T) {
	tests := []struct {
		name   string
		ids    []book.RequestID
		target book.RequestID
		freeze bool
		want   []string
	}{
		{name: "interrupt stopped", ids: []book.RequestID{"b", "c"}, target: "c", freeze: true, want: []string{"c", "b"}},
		{name: "idle", ids: []book.RequestID{"b"}, target: "b", want: []string{"b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
			for _, id := range test.ids {
				row := &book.Request{ID: id, Sender: "caller", Input: runtimeproto.Input{SourceID: string(id), Text: string(id)}, Bytes: len(id), Location: book.Buffered, Scope: l.vault.Mint(string(id), string(id))}
				l.state.Requests[id] = row
				l.state.Buffer = append(l.state.Buffer, id)
				l.state.BufferBytes += row.Bytes
			}
			if test.freeze {
				l.freezeInterrupt("stop")
			}
			l.handleIntake(v7Request("insert", TypeSteer, "caller", `{"target":"`+string(test.target)+`"}`))
			if got := sys.terminal("insert"); len(got) != 1 || got[0].fail {
				t.Fatalf("terminal=%v", got)
			}
			if l.frozen(l.now()) || len(rt.controls) != 0 || len(rt.starts) != 1 || l.state.BufferBytes != 0 {
				t.Fatalf("frozen=%v starts=%+v controls=%v bytes=%d", l.frozen(l.now()), rt.starts, rt.controls, l.state.BufferBytes)
			}
			got := make([]string, 0, len(rt.starts[0].Messages))
			for _, input := range rt.starts[0].Messages {
				got = append(got, input.SourceID)
			}
			if !slices.Equal(got, test.want) || l.state.Turn.Owner != test.ids[0] {
				t.Fatalf("messages=%v want=%v turn=%+v", got, test.want, l.state.Turn)
			}
		})
	}
}

func TestAgentControl37SteerTargetOwnershipAndOriginalIndexReturn(t *testing.T) {
	seed := func(t *testing.T) (*agentLoop, *v7Sys, *captureRuntime) {
		t.Helper()
		l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		v7Activate(t, l, "owner")
		for _, item := range []struct{ id, text string }{{"before", "before"}, {"target", "insert me"}, {"after", "after"}} {
			l.handleIntake(v7Request(item.id, TypeAsk, "caller", `{"text":"`+item.text+`"}`))
		}
		l.handleIntake(v7Request("insert", TypeSteer, "caller", `{"target":"target"}`))
		if got := sys.terminal("insert"); len(got) != 1 || got[0].fail {
			t.Fatalf("insert terminal=%v", got)
		}
		if len(rt.controls) != 1 || rt.controls[0].Content == nil || rt.controls[0].Content.Text != "insert me" {
			t.Fatalf("controls=%+v", rt.controls)
		}
		return l, sys, rt
	}

	t.Run("accepted transfers owner and preempts old owner", func(t *testing.T) {
		l, sys, _ := seed(t)
		a := l.state.Running
		l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted})
		if l.state.Turn.Owner != "target" || l.state.Requests["target"].Location != book.Workspace {
			t.Fatalf("turn=%+v target=%+v", l.state.Turn, l.state.Requests["target"])
		}
		old := sys.terminal("owner")
		if len(old) != 1 || old[0].fail || old[0].value.(map[string]any)["preempted_by"] != book.RequestID("target") {
			t.Fatalf("owner terminal=%v", old)
		}
	})

	for _, verdict := range []runtimeproto.ControlVerdict{runtimeproto.ControlRejected, runtimeproto.ControlTimeout} {
		t.Run(string(verdict)+" returns original index", func(t *testing.T) {
			l, sys, _ := seed(t)
			a := l.state.Running
			l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: verdict})
			want := []book.RequestID{"before", "target", "after"}
			if len(l.state.Buffer) != len(want) {
				t.Fatalf("buffer=%v", l.state.Buffer)
			}
			for i := range want {
				if l.state.Buffer[i] != want[i] {
					t.Fatalf("buffer=%v want=%v", l.state.Buffer, want)
				}
			}
			if row := l.state.Requests["target"]; row == nil || row.Location != book.Buffered || len(sys.terminal("target")) != 0 {
				t.Fatalf("target=%+v terminal=%v", row, sys.terminal("target"))
			}
		})
	}
}

func TestAgentControl38SteerAllActiveTurnSettlesBatchAndPreservesOthers(t *testing.T) {
	seed := func(t *testing.T) (*agentLoop, *v7Sys, *captureRuntime) {
		t.Helper()
		l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		v7Activate(t, l, "owner")
		l.handleIntake(v7Request("own-1", TypeAsk, "caller", `{"text":"one"}`))
		l.handleIntake(v7Request("other", TypeAsk, "other", `{"text":"leave me"}`))
		l.handleIntake(v7Request("own-2", TypeAsk, "caller", `{"text":"two"}`))
		l.handleIntake(v7Request("insert-all", TypeSteer, "caller", `{"all":true}`))
		if got := sys.terminal("insert-all"); len(got) != 1 || got[0].fail {
			t.Fatalf("terminal=%v", got)
		}
		if !slices.Equal(l.state.Buffer, []book.RequestID{"other"}) || len(rt.controls) != 1 || rt.controls[0].Content == nil || rt.controls[0].Content.Text != "one\n\ntwo" {
			t.Fatalf("buffer=%v controls=%+v", l.state.Buffer, rt.controls)
		}
		return l, sys, rt
	}

	t.Run("accepted", func(t *testing.T) {
		l, sys, _ := seed(t)
		a := l.state.Running
		l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: runtimeproto.ControlAccepted})
		merged := sys.terminal("own-1")
		preempted := sys.terminal("owner")
		if len(merged) != 1 || merged[0].value.(map[string]any)["merged_into"] != book.RequestID("own-2") ||
			len(preempted) != 1 || preempted[0].value.(map[string]any)["preempted_by"] != book.RequestID("own-2") ||
			l.state.Turn.Owner != "own-2" || l.state.Requests["own-2"].Location != book.Workspace || l.state.Requests["other"].Location != book.Buffered {
			t.Fatalf("merged=%v preempted=%v turn=%+v buffer=%v", merged, preempted, l.state.Turn, l.state.Buffer)
		}
	})

	for _, verdict := range []runtimeproto.ControlVerdict{runtimeproto.ControlRejected, runtimeproto.ControlTimeout} {
		t.Run(string(verdict)+" returns every item", func(t *testing.T) {
			l, _, _ := seed(t)
			a := l.state.Running
			l.onControlDone(runtimeEvent{kind: evControlDone, op: a.Op, turnID: a.Target, verdict: verdict})
			want := []book.RequestID{"own-1", "other", "own-2"}
			if !slices.Equal(l.state.Buffer, want) {
				t.Fatalf("buffer=%v want=%v", l.state.Buffer, want)
			}
			for _, id := range []book.RequestID{"own-1", "own-2"} {
				if row := l.state.Requests[id]; row == nil || row.Location != book.Buffered {
					t.Fatalf("%s=%+v", id, row)
				}
			}
		})
	}
}

func TestAgentControl39SteerAllWithoutTurnStartsOwnBatchOrNoOps(t *testing.T) {
	t.Run("stopped", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		l.state.Pending = &book.Action{Kind: book.ActionCleanup}
		l.handleIntake(v7Request("other", TypeAsk, "other", `{"text":"other"}`))
		l.handleIntake(v7Request("own-1", TypeAsk, "caller", `{"text":"one"}`))
		l.handleIntake(v7Request("own-2", TypeAsk, "caller", `{"text":"two"}`))
		l.state.Pending = nil
		l.freezeInterrupt("stop")
		l.handleIntake(v7Request("insert-all", TypeSteer, "caller", `{"all":true}`))
		if got := sys.terminal("insert-all"); len(got) != 1 || got[0].fail || l.frozen(l.now()) {
			t.Fatalf("terminal=%v frozen=%v", got, l.frozen(l.now()))
		}
		if len(rt.starts) != 1 || len(rt.starts[0].Messages) != 2 || rt.starts[0].Messages[0].SourceID != "own-1" || rt.starts[0].Messages[1].SourceID != "own-2" || l.state.Turn.Owner != "own-2" || !slices.Equal(l.state.Buffer, []book.RequestID{"other"}) {
			t.Fatalf("starts=%+v turn=%+v buffer=%v", rt.starts, l.state.Turn, l.state.Buffer)
		}
	})

	t.Run("empty own buffer", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		l.freezeInterrupt("stop")
		l.handleIntake(v7Request("insert-all", TypeSteer, "caller", `{"all":true}`))
		if got := sys.terminal("insert-all"); len(got) != 1 || got[0].fail || !l.frozen(l.now()) || len(rt.starts) != 0 || len(rt.controls) != 0 {
			t.Fatalf("terminal=%v frozen=%v starts=%v controls=%v", got, l.frozen(l.now()), rt.starts, rt.controls)
		}
	})
}

func TestAgentControl40HoldRestoresInterruptFreeze(t *testing.T) {
	t.Run("unhold after replace", func(t *testing.T) {
		l, sys, rt := newV7Loop(t, nil)
		l.state.Pending = &book.Action{Kind: book.ActionCleanup}
		l.handleIntake(v7Request("target", TypeAsk, "caller", `{"text":"old"}`))
		l.state.Pending = nil
		l.handleIntake(v7Request("stop", TypeInterrupt, "caller", `{}`))
		l.handleIntake(v7Request("hold-1", TypeHold, "caller", `{"target":"target"}`))
		l.handleIntake(v7Request("hold-2", TypeHold, "caller", `{"target":"target"}`))
		l.handleIntake(v7Request("replacement", TypeReplace, "caller", `{"target":"target","old_text":"old","new_text":"new"}`))
		if !l.restoreInterrupt || l.freezeSource != freezeSourceHold || len(rt.starts) != 0 {
			t.Fatalf("before unhold restore=%v source=%v starts=%v", l.restoreInterrupt, l.freezeSource, rt.starts)
		}
		l.handleIntake(v7Request("unhold", TypeUnhold, "caller", `{}`))
		if got := sys.terminal("unhold"); len(got) != 1 || got[0].fail || !l.frozen(l.now()) || l.freezeSource != freezeSourceInterrupt || l.heldBy != "stop" || l.restoreInterrupt || len(rt.starts) != 0 {
			t.Fatalf("terminal=%v frozen=%v source=%v heldBy=%q restore=%v starts=%v", got, l.frozen(l.now()), l.freezeSource, l.heldBy, l.restoreInterrupt, rt.starts)
		}
		l.handleIntake(v7Request("resume", TypeAsk, "caller", `{"text":"continue"}`))
		if l.frozen(l.now()) || l.freezeSource != freezeSourceNone || len(rt.starts) != 1 || len(rt.starts[0].Messages) != 2 {
			t.Fatalf("frozen=%v source=%v starts=%+v", l.frozen(l.now()), l.freezeSource, rt.starts)
		}
	})

	t.Run("expiry", func(t *testing.T) {
		l, _, rt := newV7Loop(t, nil)
		now := time.Unix(100, 0)
		l.nowFn = func() time.Time { return now }
		l.handleIntake(v7Request("stop", TypeInterrupt, "caller", `{}`))
		l.handleIntake(v7Request("hold", TypeHold, "caller", `{"duration_ms":1000}`))
		now = now.Add(2 * time.Second)
		l.handleHoldExpired(json.RawMessage(`{"hold_id":"hold"}`))
		if !l.frozen(now) || l.freezeSource != freezeSourceInterrupt || l.heldBy != "stop" || l.restoreInterrupt || len(rt.starts) != 0 {
			t.Fatalf("frozen=%v source=%v heldBy=%q restore=%v starts=%v", l.frozen(now), l.freezeSource, l.heldBy, l.restoreInterrupt, rt.starts)
		}
	})
}

func TestAgentControl41SteerAllPayloadValidation(t *testing.T) {
	for _, payload := range []string{`{"all":false}`, `{"all":true,"target":"target"}`} {
		t.Run(payload, func(t *testing.T) {
			l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
			l.handleIntake(v7Request("insert", TypeSteer, "caller", payload))
			if got := sys.terminal("insert"); len(got) != 1 || got[0].code != "invalid_args" {
				t.Fatalf("terminal=%v", got)
			}
		})
	}
}
