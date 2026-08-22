package base

import (
	"testing"

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
