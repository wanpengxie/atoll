package base

import "testing"

func TestControlDeadlineSettlesByVerb(t *testing.T) {
	for _, typ := range []string{TypeInterrupt, TypeStop, TypeTerminate, TypeRestart} {
		t.Run(typ, func(t *testing.T) {
			l, e := newUnitLoop()
			l.state, l.turnID = stateTurnActive, "turn"
			l.active = bufferedMsg("active", "actor:a", 1)
			slot := &controlSlot{kind: typ, item: bufferedControl("control", typ), op: "deadline-op"}
			l.executingControl = slot
			l.expireControl(slot.op)
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
