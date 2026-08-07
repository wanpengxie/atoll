package base

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestSteerAcceptedTransfersSoleEffectiveOwner(t *testing.T) {
	l, _ := newUnitLoop()
	old := bufferedMsg("old", "actor:a", 1)
	newer := bufferedMsg("new", "actor:a", 1)
	l.state, l.turnID, l.active, l.lastOwner = stateTurnActive, "turn-1", old, old
	l.acceptContent(newer, true)
	var op OpID
	for id := range l.committing {
		op = id
	}
	l.controlDone(providerEvent{op: op, verdict: ControlAccepted, turnID: "turn-1"})
	if l.active != newer || l.lastOwner != newer {
		t.Fatalf("owner active=%p last=%p, want new=%p", l.active, l.lastOwner, newer)
	}
	terms := l.sys.(*testSys).terms
	if len(terms) != 1 || terms[0].id != "old" || terms[0].kind != "reply" {
		t.Fatalf("preemption terminal=%#v", terms)
	}
	value := terms[0].value.(map[string]any)
	if value["preempted_by"] != message.ID("new") {
		t.Fatalf("preemption value=%#v", value)
	}
}

func TestNaturalCompletionWaitsForLateSteerBeforeSettling(t *testing.T) {
	l, _ := newUnitLoop()
	old := bufferedMsg("old", "actor:a", 1)
	newer := bufferedMsg("new", "actor:a", 1)
	l.state, l.turnID, l.active, l.lastOwner = stateTurnActive, "turn-1", old, old
	l.acceptContent(newer, true)
	var op OpID
	for id := range l.committing {
		op = id
	}
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn-1", status: TurnStatusOK, finalText: "answer"})
	if !l.settling || l.state != stateTurnActive {
		t.Fatalf("completion did not enter settling: state=%v settling=%v", l.state, l.settling)
	}
	l.controlDone(providerEvent{op: op, verdict: ControlAccepted, turnID: "turn-1"})
	terms := l.sys.(*testSys).terms
	if len(terms) != 2 || terms[0].id != "old" || terms[1].id != "new" {
		t.Fatalf("terminal order=%#v", terms)
	}
	if len(l.sys.(*testSys).emits) != 1 || l.sys.(*testSys).emits[0].Type != "activity.turn.ended" {
		t.Fatalf("ended activity=%#v", l.sys.(*testSys).emits)
	}
}
