package base

import (
	"testing"
)

// The slot's enforcement clock runs from enslotment to slot terminal, and
// expiry escalates uniformly: reap the connection, settle work accounts by
// the verb's own semantics, close the slot loudly.

func TestControlDeadlineSettlesByVerb(t *testing.T) {
	for _, typ := range []string{TypeInterrupt, TypeStop, TypeTerminate, TypeRestart} {
		t.Run(typ, func(t *testing.T) {
			l, e := newUnitLoop()
			l.state, l.turnID = stateTurnActive, "turn"
			l.active = bufferedMsg("active", "actor:a", 1)
			slot := &controlSlot{kind: typ, item: bufferedControl("control", typ), op: "deadline-op"}
			l.executingControl = slot
			l.expireControl(slot)
			// Escalation reaps the connection for every verb. A terminate slot
			// can only expire while its action was never reached (parked or
			// hung upstream), so running Terminate IS the escalation there too.
			if e.terminates != 1 || l.executingControl != nil || l.state != stateIdle {
				t.Fatalf("terminate=%d executing=%#v state=%v", e.terminates, l.executingControl, l.state)
			}
			terms := l.sys.(*testSys).terms
			want := errorCancelled
			if typ == TypeInterrupt {
				want = errorInterrupted
			}
			if len(terms) < 2 || terms[0].id != "active" || terms[0].code != want {
				t.Fatalf("work terminal=%#v want=%s", terms, want)
			}
		})
	}
}

// A slot parked pending — here an interrupt waiting out a starting turn that
// never reports TurnStarted — expires on the same clock as an executing one:
// the deadline is armed at enslotment, not at RPC submission.
func TestPendingControlDeadlineIsReachable(t *testing.T) {
	l, e := newUnitLoop()
	l.enqueue(bufferedMsg("first", "actor:a", 1), false)
	if l.state != stateStarting {
		t.Fatalf("setup state=%v", l.state)
	}
	l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
	slot := l.pendingControl
	if slot == nil {
		t.Fatal("interrupt was not parked pending during starting")
	}
	if slot.timer == nil {
		t.Fatal("pending slot has no deadline clock — the bound runs from acceptance")
	}
	l.expireControl(slot)
	if l.pendingControl != nil || e.terminates != 1 || l.state != stateIdle {
		t.Fatalf("pending expiry did not escalate: pending=%#v terminates=%d state=%v", l.pendingControl, e.terminates, l.state)
	}
	if !hasTerminal(l.sys.(*testSys), "interrupt") {
		t.Fatal("expired pending control request remained open")
	}
	if !hasTerminal(l.sys.(*testSys), "first") {
		t.Fatal("starting turn's request remained open after escalation")
	}
}

func TestStopControlDeadlineIsReachable(t *testing.T) {
	l, engine := newUnitLoop()
	l.state, l.turnID = stateTurnActive, "turn"
	l.active = bufferedMsg("active", "actor:a", 1)
	l.acceptControl(bufferedControl("stop", TypeStop))
	slot := l.executingControl
	if slot == nil || slot.op == "" || !slot.item.closed {
		t.Fatalf("stop slot=%#v", slot)
	}
	terminalsBefore := len(l.sys.(*testSys).terms)
	l.expireControl(slot)
	if l.executingControl != nil || l.state != stateIdle || engine.terminates != 1 {
		t.Fatalf("deadline did not harvest stop: control=%#v state=%v terminates=%d", l.executingControl, l.state, engine.terminates)
	}
	if got := len(l.sys.(*testSys).terms); got != terminalsBefore {
		t.Fatalf("mechanically completed stop got a second terminal: before=%d after=%d", terminalsBefore, got)
	}
}

// A timer that fires after its slot already reached a terminal must be a
// no-op: aliveness is checked against the live pending/executing slots, so a
// stale fire cannot reap a healthy connection or double-settle accounts.
func TestStaleSlotExpiryIsIgnored(t *testing.T) {
	l, e := newUnitLoop()
	slot := &controlSlot{kind: TypeStop, item: bufferedControl("stop", TypeStop)}
	l.expireControl(slot)
	if e.terminates != 0 || len(l.sys.(*testSys).terms) != 0 {
		t.Fatalf("stale expiry acted: terminates=%d terms=%#v", e.terminates, l.sys.(*testSys).terms)
	}
}
