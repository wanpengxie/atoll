package base

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestStopClearsBufferCommittingAndWorkspace(t *testing.T) {
	l, e := newUnitLoop()
	l.state, l.turnID = stateTurnActive, "turn"
	l.active, l.lastOwner = bufferedMsg("active", "actor:a", 1), bufferedMsg("last", "actor:a", 1)
	l.buffer.push(bufferedMsg("queued", "actor:a", 1))
	l.committing["op-steer"] = &operation{kind: TypeSteer, item: bufferedMsg("committing", "actor:a", 1)}
	l.acceptControl(bufferedControl("stop", TypeStop))
	if l.active != nil || len(l.buffer.items) != 0 || len(l.committing) != 0 || l.state != stateInterrupting || e.terminates != 0 || len(e.interrupts) != 1 {
		t.Fatalf("work remains active=%#v buffer=%#v committing=%#v state=%v", l.active, l.buffer.items, l.committing, l.state)
	}
	if len(l.sys.(*testSys).terms) != 4 {
		t.Fatalf("terminals=%#v", l.sys.(*testSys).terms)
	}
	if l.executingControl == nil || l.executingControl.kind != TypeStop {
		t.Fatalf("stop fence not retained: %#v", l.executingControl)
	}
	l.finishExecutingControl(providerEvent{op: e.interrupts[0], verdict: ControlAccepted})
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn", status: TurnStatusInterrupted})
	if l.state != stateIdle || l.executingControl != nil || e.terminates != 0 {
		t.Fatalf("stop did not settle cleanly: state=%v control=%#v terminates=%d", l.state, l.executingControl, e.terminates)
	}
}

// A control that arrives while a turn is starting waits for that turn to land
// in the record before acting. Acting early would leave the log with a turn
// that was killed but never seen to begin, and would freeze a window that is
// still supposed to be supersedable.
func TestControlDuringStartingWaitsForTheTurnToBeRecorded(t *testing.T) {
	l, engine := newUnitLoop()
	l.enqueue(bufferedMsg("starting", "actor:a", 1), false)
	if l.state != stateStarting {
		t.Fatalf("setup state=%v", l.state)
	}
	l.acceptControl(bufferedControl("stop", TypeStop))
	if l.pendingControl == nil || l.executingControl != nil {
		t.Fatalf("stop acted inside the starting window: pending=%#v executing=%#v", l.pendingControl, l.executingControl)
	}
	if hasTerminal(l.sys.(*testSys), "stop") || len(engine.interrupts) != 0 || engine.terminates != 0 {
		t.Fatal("stop produced effects before the turn was recorded")
	}
	// A later control may still replace it — that is what waiting buys.
	l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
	if l.pendingControl.kind != TypeInterrupt || !hasTerminal(l.sys.(*testSys), "stop") {
		t.Fatalf("pending control was not supersedable: pending=%#v", l.pendingControl)
	}

	l.handleProviderEvent(providerEvent{kind: eventTurnStarted, op: OpID("op-1"), turnID: "turn-1"})
	sys := l.sys.(*testSys)
	if len(sys.emits) != 1 || sys.emits[0].Type != "activity.turn.started" {
		t.Fatalf("the turn was not recorded before the control ran: emits=%#v", sys.emits)
	}
	if l.lastOwner == nil {
		t.Fatal("turn has no owner anchor")
	}
	if len(engine.interrupts) != 1 || l.state != stateInterrupting {
		t.Fatalf("control did not run once the turn landed: interrupts=%v state=%v", engine.interrupts, l.state)
	}
	l.finishExecutingControl(providerEvent{op: engine.interrupts[0], verdict: ControlAccepted})
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn-1", status: TurnStatusInterrupted})
	if l.state != stateIdle || l.executingControl != nil || engine.terminates != 0 {
		t.Fatalf("control did not settle: state=%v control=%#v terminates=%d", l.state, l.executingControl, engine.terminates)
	}
	ended := false
	for _, emit := range sys.emits {
		ended = ended || emit.Type == "activity.turn.ended"
	}
	if !ended {
		t.Fatalf("turn phase left open: emits=%#v", sys.emits)
	}
}

// A start that never lands must not park the control forever: the slot's own
// deadline escalates to a connection reap.
func TestControlPendingOnStartingExpiresIntoAReap(t *testing.T) {
	l, engine := newUnitLoop()
	l.enqueue(bufferedMsg("starting", "actor:a", 1), false)
	l.acceptControl(bufferedControl("stop", TypeStop))
	slot := l.pendingControl
	if slot == nil || slot.timer == nil {
		t.Fatalf("pending stop has no deadline: %#v", slot)
	}
	l.expireControl(slot)
	if l.pendingControl != nil || engine.terminates != 1 || l.state != stateIdle {
		t.Fatalf("expiry did not escalate: pending=%#v terminates=%d state=%v", l.pendingControl, engine.terminates, l.state)
	}
}

func TestStopWaitsForInterruptRPCWhenTurnEndsFirst(t *testing.T) {
	l, engine := newUnitLoop()
	l.state, l.turnID = stateTurnActive, "turn"
	l.active = bufferedMsg("active", "actor:a", 1)
	l.acceptControl(bufferedControl("stop", TypeStop))
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn", status: TurnStatusInterrupted})
	if !l.settling || l.executingControl == nil || l.state == stateIdle {
		t.Fatalf("turn end crossed stop RPC fence: settling=%v control=%#v state=%v", l.settling, l.executingControl, l.state)
	}
	l.finishExecutingControl(providerEvent{op: engine.interrupts[0], verdict: ControlAccepted})
	if l.settling || l.executingControl != nil || l.state != stateIdle || engine.terminates != 0 {
		t.Fatalf("stop did not settle after RPC: settling=%v control=%#v state=%v terminates=%d", l.settling, l.executingControl, l.state, engine.terminates)
	}
}

func TestRestartWaitsForEnsureAliveAndPreservesBuffer(t *testing.T) {
	l, e := newUnitLoop()
	l.state = stateTurnActive
	l.active = bufferedMsg("active", "actor:a", 1)
	l.buffer.push(bufferedMsg("queued", "actor:a", 1))
	// Terminate is contractually non-blocking, so the whole terminate phase of
	// restart completes inline within acceptControl; only EnsureAlive is async.
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

// The two mandatory failure forms that survive steer's degradation rule: an
// explicit expected_turn_id never degrades silently, and empty input never
// starts or joins a turn.
func TestSteerDegradationKeepsMandatoryFailureForms(t *testing.T) {
	t.Run("explicit CAS fails when provider cannot steer", func(t *testing.T) {
		l, e := newUnitLoop()
		delete(l.def.controls, TypeSteer) // provider without a steer primitive
		l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("active", "actor:a", 1)
		item := bufferedControl("steer", TypeSteer)
		item.explicitCAS = true
		l.acceptContent(item, true)
		terms := l.sys.(*testSys).terms
		if len(terms) != 1 || terms[0].code != errorCASMismatch {
			t.Fatalf("explicit CAS degraded into the queue: terms=%#v", terms)
		}
		if len(l.buffer.items) != 0 || len(e.steers) != 0 {
			t.Fatalf("explicit CAS item was queued: buffer=%d steers=%v", len(l.buffer.items), e.steers)
		}
	})
	t.Run("empty steer fails in every state", func(t *testing.T) {
		for name, prepare := range map[string]func(*agentLoop){
			"idle":   func(*agentLoop) {},
			"active": func(l *agentLoop) { l.state, l.turnID = stateTurnActive, "turn" },
		} {
			t.Run(name, func(t *testing.T) {
				l, e := newUnitLoop()
				prepare(l)
				msg := testMsg(message.KindRequest, "actor:a", TypeSteer)
				msg.Envelope.Payload = []byte(`{"text":"   "}`)
				l.handleIntake(msg, make(chan closureEvent, 1))
				terms := l.sys.(*testSys).terms
				if len(terms) != 1 || terms[0].code != errorEmptyInput {
					t.Fatalf("empty steer terminal=%#v", terms)
				}
				if e.starts != 0 || len(e.steers) != 0 || len(l.buffer.items) != 0 {
					t.Fatalf("empty steer reached the provider: starts=%d steers=%v buffer=%d", e.starts, e.steers, len(l.buffer.items))
				}
			})
		}
	})
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

func TestInterruptNoActiveWaitsForAuthoritativeTurnEnd(t *testing.T) {
	l, e := newUnitLoop()
	owner := bufferedMsg("active", "actor:a", 1)
	l.state, l.turnID, l.active, l.lastOwner = stateTurnActive, "turn", owner, owner
	l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
	if len(e.interrupts) != 1 {
		t.Fatalf("interrupts=%#v", e.interrupts)
	}

	l.finishExecutingControl(providerEvent{op: e.interrupts[0], verdict: ControlNoActiveTurn})
	if l.state != stateInterrupting || l.turnID != "turn" || l.active != owner || l.executingControl == nil {
		t.Fatalf("no-active response dropped workspace: state=%v turn=%q active=%#v control=%#v", l.state, l.turnID, l.active, l.executingControl)
	}
	if hasTerminal(l.sys.(*testSys), "active") || hasTerminal(l.sys.(*testSys), "interrupt") {
		t.Fatalf("request settled before authoritative turn end: %#v", l.sys.(*testSys).terms)
	}

	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn", status: TurnStatusOK, finalText: "done"})
	if l.state != stateIdle || l.turnID != "" || l.active != nil || l.executingControl != nil {
		t.Fatalf("turn did not settle: state=%v turn=%q active=%#v control=%#v", l.state, l.turnID, l.active, l.executingControl)
	}
	if !hasTerminal(l.sys.(*testSys), "active") || !hasTerminal(l.sys.(*testSys), "interrupt") {
		t.Fatalf("settlement terminals=%#v", l.sys.(*testSys).terms)
	}
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
