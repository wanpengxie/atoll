package base

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestPendingControlSupersededGetsCompletedTerminal(t *testing.T) {
	l, _ := newUnitLoop()
	l.state = stateTurnActive
	l.settling = true
	first := bufferedControl("c1", TypeStop)
	second := bufferedControl("c2", TypeRestart)
	l.acceptControl(first)
	l.acceptControl(second)
	if l.pendingControl == nil || l.pendingControl.item != second {
		t.Fatalf("pending=%#v, want second", l.pendingControl)
	}
	terms := l.sys.(*testSys).terms
	if len(terms) != 1 || terms[0].id != "c1" || terms[0].kind != "reply" {
		t.Fatalf("superseded terminal=%#v", terms)
	}
	if terms[0].value.(map[string]any)["superseded_by"] != message.ID("c2") {
		t.Fatalf("superseded value=%#v", terms[0].value)
	}
}

func TestExecutingControlCannotBeSuperseded(t *testing.T) {
	l, _ := newUnitLoop()
	l.state = stateTurnActive
	l.turnID = "turn-1"
	first := bufferedControl("c1", TypeInterrupt)
	second := bufferedControl("c2", TypeRestart)
	l.acceptControl(first)
	if l.executingControl == nil || l.executingControl.item != first {
		t.Fatalf("first was not executing: %#v", l.executingControl)
	}
	l.acceptControl(second)
	if l.executingControl.item != first || l.pendingControl == nil || l.pendingControl.item != second {
		t.Fatalf("slots executing=%#v pending=%#v", l.executingControl, l.pendingControl)
	}
	if len(l.sys.(*testSys).terms) != 0 {
		t.Fatalf("executing control was terminalized: %#v", l.sys.(*testSys).terms)
	}
}

func bufferedControl(id, typ string) *requestItem {
	item := bufferedMsg(id, "actor:user", 1)
	item.msg.Type = typ
	item.trigger.Envelope.Type = typ
	return item
}
