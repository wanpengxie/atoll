package base

import "testing"

func TestProviderLostSettlesFiveAccountGroups(t *testing.T) {
	l, _ := newUnitLoop()
	l.state, l.turnID = stateTurnActive, "turn"
	l.active, l.lastOwner = bufferedMsg("active", "actor:a", 1), bufferedMsg("last", "actor:a", 1)
	l.committing["start"] = &operation{kind: "start", item: bufferedMsg("starting", "actor:a", 1)}
	l.committing["steer"] = &operation{kind: TypeSteer, item: bufferedMsg("steer", "actor:a", 1)}
	l.executingControl = &controlSlot{kind: TypeInterrupt, item: bufferedControl("control", TypeInterrupt), op: "control"}
	l.pendingControl = &controlSlot{kind: TypeRestart, item: bufferedControl("pending", TypeRestart)}
	l.buffer.push(bufferedMsg("buffered", "actor:a", 1))
	l.providerLost(LostCrash, "eof")
	for _, id := range []string{"active", "starting", "steer", "control"} {
		if !hasTerminal(l.sys.(*testSys), id) {
			t.Fatalf("account %q remains open: %#v", id, l.sys.(*testSys).terms)
		}
	}
	if hasTerminal(l.sys.(*testSys), "buffered") || hasTerminal(l.sys.(*testSys), "pending") {
		t.Fatalf("preserved account was closed: %#v", l.sys.(*testSys).terms)
	}
}

func TestShutdownWritesNoTerminal(t *testing.T) {
	l, _ := newUnitLoop()
	l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("active", "actor:a", 1)
	port := &eventPort{life: l.sys.Life(), events: make(chan providerEvent, 1), sys: l.sys}
	port.closed.Store(true)
	port.ProviderLost(LostCrash, "shutdown eof")
	if len(l.sys.(*testSys).terms) != 0 {
		t.Fatalf("shutdown synthesized terminals: %#v", l.sys.(*testSys).terms)
	}
}
