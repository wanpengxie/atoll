package base

import "testing"

func TestStopClearsBufferCommittingAndWorkspace(t *testing.T) {
	l, e := newUnitLoop()
	l.state = stateTurnActive
	l.active, l.lastOwner = bufferedMsg("active", "actor:a", 1), bufferedMsg("last", "actor:a", 1)
	l.buffer.push(bufferedMsg("queued", "actor:a", 1))
	l.committing["op-steer"] = &operation{kind: TypeSteer, item: bufferedMsg("committing", "actor:a", 1)}
	l.acceptControl(bufferedControl("stop", TypeStop))
	if l.active != nil || len(l.buffer.items) != 0 || len(l.committing) != 0 || l.state != stateIdle || e.terminates != 1 {
		t.Fatalf("work remains active=%#v buffer=%#v committing=%#v state=%v", l.active, l.buffer.items, l.committing, l.state)
	}
	if len(l.sys.(*testSys).terms) != 4 {
		t.Fatalf("terminals=%#v", l.sys.(*testSys).terms)
	}
}

func TestStopFencesStartingTurnBeforeReply(t *testing.T) {
	l, engine := newUnitLoop()
	l.state = stateStarting
	l.committing["start"] = &operation{kind: "start", item: bufferedMsg("starting", "actor:a", 1)}
	l.acceptControl(bufferedControl("stop", TypeStop))
	if engine.terminates != 1 || l.state != stateIdle || len(l.committing) != 0 {
		t.Fatalf("stop did not fence start: terminates=%d state=%v committing=%v", engine.terminates, l.state, l.committing)
	}
	if !hasTerminal(l.sys.(*testSys), "stop") {
		t.Fatal("stop did not reply after provider fence")
	}
}

func TestRestartWaitsForEnsureAliveAndPreservesBuffer(t *testing.T) {
	l, e := newUnitLoop()
	l.state = stateTurnActive
	l.active = bufferedMsg("active", "actor:a", 1)
	l.buffer.push(bufferedMsg("queued", "actor:a", 1))
	l.acceptControl(bufferedControl("restart", TypeRestart))
	if l.executingControl == nil || len(e.ensures) != 1 || len(l.buffer.items) != 1 {
		t.Fatalf("restart executing=%#v ensures=%#v buffer=%#v", l.executingControl, e.ensures, l.buffer.items)
	}
	if hasTerminal(l.sys.(*testSys), "restart") {
		t.Fatal("restart replied before EnsureAlive")
	}
	l.finishExecutingControl(providerEvent{op: e.ensures[0], verdict: ControlAccepted})
	if !hasTerminal(l.sys.(*testSys), "restart") {
		t.Fatal("restart did not reply after EnsureAlive")
	}
}

func TestTerminateThenNextRequestRestarts(t *testing.T) {
	l, e := newUnitLoop()
	l.state = stateTurnActive
	l.active = bufferedMsg("active", "actor:a", 1)
	l.acceptControl(bufferedControl("terminate", TypeTerminate))
	if e.terminates != 1 || l.state != stateIdle {
		t.Fatalf("terminate count=%d state=%v", e.terminates, l.state)
	}
	l.enqueue(bufferedMsg("next", "actor:a", 1), false)
	if e.starts != 1 {
		t.Fatalf("next request starts=%d", e.starts)
	}
}

func TestExplicitCASMismatchFails(t *testing.T) {
	l, _ := newUnitLoop()
	item := bufferedControl("steer", TypeSteer)
	item.explicitCAS = true
	l.acceptContent(item, true)
	terms := l.sys.(*testSys).terms
	if len(terms) != 1 || terms[0].code != "cas_mismatch" {
		t.Fatalf("terminals=%#v", terms)
	}
}

func TestInterruptForms(t *testing.T) {
	t.Run("idle empty is idempotent", func(t *testing.T) {
		l, _ := newUnitLoop()
		l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
		if !hasTerminal(l.sys.(*testSys), "interrupt") {
			t.Fatal("idle interrupt did not complete")
		}
	})
	t.Run("active submits interrupt", func(t *testing.T) {
		l, e := newUnitLoop()
		l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("active", "actor:a", 1)
		l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
		if len(e.interrupts) != 1 || l.state != stateInterrupting {
			t.Fatalf("interrupts=%#v state=%v", e.interrupts, l.state)
		}
	})
}

func TestLateAcceptedSteerIsCancelledWhenControlPending(t *testing.T) {
	l, _ := newUnitLoop()
	l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("old", "actor:a", 1)
	steer := bufferedMsg("steer", "actor:a", 1)
	l.acceptContent(steer, true)
	var steerOp OpID
	for id := range l.committing {
		steerOp = id
	}
	l.settling = true // close the control execution window while retaining the in-flight steer
	l.acceptControl(bufferedControl("stop", TypeStop))
	l.controlDone(providerEvent{op: steerOp, verdict: ControlAccepted, turnID: "turn"})
	terms := l.sys.(*testSys).terms
	if len(terms) != 1 || terms[0].id != "steer" || terms[0].code != "cancelled" || l.active.msg.ID != "old" {
		t.Fatalf("late steer outcome terms=%#v active=%#v", terms, l.active)
	}
}

func hasTerminal(sys *testSys, id string) bool {
	for _, term := range sys.terms {
		if string(term.id) == id {
			return true
		}
	}
	return false
}
