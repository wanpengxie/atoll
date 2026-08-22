package base

import (
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// The successful select path persists asynchronously (persistSelection runs a
// goroutine against sys.State()); the shared v7 harness embeds a nil Sys, so
// without this stub that goroutine would panic the whole test process.
func (s *v7Sys) State() actorbase.StateHandle { return v7StateStub{} }

type v7StateStub struct{}

func (v7StateStub) Get(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
}
func (v7StateStub) Put(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (v7StateStub) Del(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}

// The exact message sequence the web frontend drives (model params design §4):
// context reads the current options cold, a legal select runs as a TurnSelect
// turn whose OK ending makes the new pair sticky, context then reports the new
// pair, and an illegal pair fails with invalid_args leaving options untouched.
func TestSelectSequenceMatchesFrontendFlow(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	selections := []runtimeproto.TurnOptions{{Model: "gpt-a", Effort: "medium"}, {Model: "gpt-b", Effort: "high"}}
	l.def.cfg.Runtime.Selections = selections
	l.options = selections[0]
	l.lastUsage, l.hasUsage = runtimeproto.TurnUsage{Model: "gpt-a", Effort: "medium"}, true

	// ① Cold start: the current pair is readable before any turn has run.
	l.handleIntake(v7Request("ctx1", TypeContext, "caller", `{}`))
	first := sys.terminal("ctx1")[0].value.(map[string]any)
	if first["model"] != "gpt-a" || first["effort"] != "medium" {
		t.Fatalf("cold context=%v", first)
	}

	// ② Legal select: runs as its own turn; OK ending makes it sticky and the
	// terminal carries the new usage.
	l.handleIntake(v7Request("sel1", TypeSelect, "caller", `{"model":"gpt-b","effort":"high"}`))
	if l.state.Turn == nil {
		t.Fatal("select did not start a turn")
	}
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn-sel1"})
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-sel1", status: runtimeproto.TurnStatusOK, usage: runtimeproto.TurnUsage{Model: "gpt-b", Effort: "high", ContextTokens: 7}})
	terminal := sys.terminal("sel1")
	if len(terminal) != 1 || terminal[0].fail {
		t.Fatalf("select terminal=%v", terminal)
	}
	usage := terminal[0].value.(map[string]any)["usage"].(map[string]any)
	if usage["model"] != "gpt-b" || usage["effort"] != "high" {
		t.Fatalf("select terminal usage=%v", usage)
	}
	if l.options != selections[1] {
		t.Fatalf("options not sticky: %+v", l.options)
	}

	// ③ Context now reports the switched pair.
	l.handleIntake(v7Request("ctx2", TypeContext, "caller", `{}`))
	second := sys.terminal("ctx2")[0].value.(map[string]any)
	if second["model"] != "gpt-b" || second["effort"] != "high" {
		t.Fatalf("post-select context=%v", second)
	}

	// ④ Selections are pairs, not a cartesian product: (gpt-b, medium) is not
	// in the catalog even though both values appear separately.
	l.handleIntake(v7Request("sel2", TypeSelect, "caller", `{"model":"gpt-b","effort":"medium"}`))
	bad := sys.terminal("sel2")
	if len(bad) != 1 || !bad[0].fail || bad[0].code != "invalid_args" {
		t.Fatalf("illegal select terminal=%v", bad)
	}
	if l.options != selections[1] {
		t.Fatalf("failed select must not move options: %+v", l.options)
	}
}

// —— 旁路独占槽（设计 §8）——————————————————————————————————

// 忙时：select 恒不入等待区；当前 turn 结束后、任何排队消息之前插队执行。
func TestSelectSlotRunsAfterCurrentTurnBeforeQueue(t *testing.T) {
	l, sys, rt := newV7Loop(t, nil)
	l.def.cfg.Runtime.Selections = []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, {Model: "m2", Effort: "high"}}
	l.options = l.def.cfg.Runtime.Selections[0]
	v7Activate(t, l, "busy")
	l.handleIntake(v7Request("queued-b", TypeAsk, "caller", `{"text":"b"}`))
	l.handleIntake(v7Request("sel", TypeSelect, "caller", `{"model":"m2","effort":"high"}`))
	if got := len(l.state.Buffer); got != 1 {
		t.Fatalf("select must not occupy the buffer; buffer=%d", got)
	}
	if progress := sys.progress["sel"]; len(progress) != 1 || progress[0].status != "queued" {
		t.Fatalf("slot registration receipt missing: %v", progress)
	}
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-busy", status: runtimeproto.TurnStatusOK, usage: runtimeproto.TurnUsage{Model: "m1", Effort: "low"}})
	if l.state.Turn == nil || l.state.Turn.Owner != book.RequestID("sel") {
		t.Fatalf("slot did not preempt the queue; turn=%+v", l.state.Turn)
	}
	if got := rt.starts; len(got) == 0 || got[len(got)-1].Kind != runtimeproto.TurnSelect {
		t.Fatalf("slot turn kind wrong: %+v", got)
	}
}

// 独占覆盖：新 select 顶掉在槽的旧 select，被顶者终态 failed/superseded（它没
// 生效过，成功终态就是撒谎）。
func TestSelectSlotSupersedesPreviousOccupant(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	l.def.cfg.Runtime.Selections = []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, {Model: "m2", Effort: "high"}}
	l.options = l.def.cfg.Runtime.Selections[0]
	v7Activate(t, l, "busy")
	l.handleIntake(v7Request("sel-old", TypeSelect, "caller", `{"model":"m2","effort":"high"}`))
	l.handleIntake(v7Request("sel-new", TypeSelect, "caller", `{"model":"m1","effort":"low"}`))
	old := sys.terminal("sel-old")
	if len(old) != 1 || !old[0].fail || old[0].code != "superseded" {
		t.Fatalf("superseded terminal=%v", old)
	}
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-busy", status: runtimeproto.TurnStatusOK, usage: runtimeproto.TurnUsage{Model: "m1", Effort: "low"}})
	if l.state.Turn == nil || l.state.Turn.Owner != book.RequestID("sel-new") {
		t.Fatalf("winner not running: %+v", l.state.Turn)
	}
}

// 停止态（interrupt 冻结）：select 直接执行——切模型恒不唤醒队列、也恒不被
// 停止拦住。slot turn 结束后队列保持冻结。
func TestSelectSlotRunsUnderInterruptFreezeWithoutWakingQueue(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	l.def.cfg.Runtime.Selections = []runtimeproto.TurnOptions{{Model: "m1", Effort: "low"}, {Model: "m2", Effort: "high"}}
	l.options = l.def.cfg.Runtime.Selections[0]
	// 真实停止流：turn 活跃时 parked 入队等待 → interrupt 冻结 → turn 被打断结束
	// → frozen 挡住 startNext → 停止态（无 turn、parked 仍在等待区）。
	v7Activate(t, l, "busy")
	l.handleIntake(v7Request("parked", TypeQueue, "caller", `{"text":"parked"}`))
	l.freezeInterrupt("stopper")
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-busy", status: runtimeproto.TurnStatusInterrupted})
	if l.state.Turn != nil || len(l.state.Buffer) != 1 {
		t.Fatalf("stop-state setup wrong: turn=%+v buffer=%d", l.state.Turn, len(l.state.Buffer))
	}
	l.handleIntake(v7Request("sel", TypeSelect, "caller", `{"model":"m2","effort":"high"}`))
	if l.state.Turn == nil || l.state.Turn.Owner != book.RequestID("sel") {
		t.Fatalf("select blocked by freeze; turn=%+v", l.state.Turn)
	}
	l.onTurnStarted(runtimeEvent{kind: evTurnStarted, op: l.state.Turn.StartOp, turnID: "turn-sel"})
	l.onTurnEnded(runtimeEvent{kind: evTurnEnded, turnID: "turn-sel", status: runtimeproto.TurnStatusOK, usage: runtimeproto.TurnUsage{Model: "m2", Effort: "high"}})
	if terminal := sys.terminal("sel"); len(terminal) != 1 || terminal[0].fail {
		t.Fatalf("select terminal=%v", terminal)
	}
	if l.options.Model != "m2" {
		t.Fatalf("not sticky: %+v", l.options)
	}
	if !l.frozen(l.now()) {
		t.Fatal("freeze was cleared by the select slot")
	}
	if l.state.Turn != nil {
		t.Fatal("queued content started despite the freeze")
	}
	if got := len(l.state.Buffer); got != 1 {
		t.Fatalf("queued content disturbed: buffer=%d", got)
	}
}

// 指令件保护：compact（仍走队列）恒不被全体插入卷走、恒不可被编辑。
func TestCommandRowsAreNotSweepableOrEditable(t *testing.T) {
	l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
	v7Activate(t, l, "busy")
	l.handleIntake(v7Request("cmd", TypeCompact, "caller", `{}`))
	if got := len(l.state.Buffer); got != 1 {
		t.Fatalf("compact should queue: %d", got)
	}
	l.handleIntake(v7Request("sweep", TypeSteer, "caller", `{"all":true}`))
	if got := len(l.state.Buffer); got != 1 {
		t.Fatalf("steer all swept a command row: buffer=%d", got)
	}
	l.handleIntake(v7Request("edit", TypeReplace, "caller", `{"target":"cmd","old_text":"","new_text":"pretend message"}`))
	if terminal := sys.terminal("edit"); len(terminal) != 1 || !terminal[0].fail || terminal[0].code != "invalid_args" {
		t.Fatalf("replace on command row terminal=%v", terminal)
	}
}
