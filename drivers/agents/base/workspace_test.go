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

// The settlement fence must hold no matter which arrival path reports last.
// Here the interrupt's ControlDone (an executing-slot path) lands while a
// steer is still in flight: settling must NOT complete, or the late steer
// would land in a turn it never participated in.
func TestExecutingSlotCompletionRespectsInFlightSteerFence(t *testing.T) {
	l, e := newUnitLoop()
	old := bufferedMsg("old", "actor:a", 1)
	newer := bufferedMsg("new", "actor:a", 1)
	l.state, l.turnID, l.active, l.lastOwner = stateTurnActive, "turn-1", old, old
	l.acceptContent(newer, true) // steer in flight
	var steerOp OpID
	for id := range l.committing {
		steerOp = id
	}
	l.acceptControl(bufferedControl("interrupt", TypeInterrupt))
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "turn-1", status: TurnStatusInterrupted})
	if !l.settling {
		t.Fatal("turn end did not enter settling behind the in-flight steer")
	}
	l.finishExecutingControl(providerEvent{op: e.interrupts[0], verdict: ControlAccepted})
	if !l.settling || l.state == stateIdle || e.starts != 0 {
		t.Fatalf("executing-slot completion crossed the fence: settling=%v state=%v starts=%d", l.settling, l.state, e.starts)
	}
	l.controlDone(providerEvent{op: steerOp, verdict: ControlAccepted, turnID: "turn-1"})
	if l.settling || l.state != stateIdle {
		t.Fatalf("settlement did not complete once the fence cleared: settling=%v state=%v", l.settling, l.state)
	}
}

// A steer whose acceptance resolves against a turn that is no longer the
// workspace's turn is silenced: its story ended with that turn, so it must
// never re-top the account of a later one.
func TestSteerAcceptedAgainstSettledTurnIsSilenced(t *testing.T) {
	l, _ := newUnitLoop()
	current := bufferedMsg("current", "actor:a", 1)
	stale := bufferedMsg("stale", "actor:a", 1)
	l.state, l.turnID, l.active, l.lastOwner = stateTurnActive, "turn-2", current, current
	l.committing["op-stale"] = &operation{kind: TypeSteer, item: stale}
	l.controlDone(providerEvent{op: "op-stale", verdict: ControlAccepted, turnID: "turn-1"})
	if l.active != current || l.lastOwner != current {
		t.Fatalf("stale steer took the workspace: active=%p want=%p", l.active, current)
	}
	terms := l.sys.(*testSys).terms
	if len(terms) != 1 || terms[0].id != "stale" || terms[0].code != errorCancelled {
		t.Fatalf("stale steer terminal=%#v", terms)
	}
}

// A turn that opened must publish a closing phase marker on EVERY path it can
// die on, not just the provider-verdict path — otherwise the channel log shows
// a turn that began and never ended.
func TestStartedTurnPhaseClosesOnEveryDeathPath(t *testing.T) {
	for name, kill := range map[string]func(*agentLoop){
		"provider lost": func(l *agentLoop) { l.providerLost(LostCrash, "boom") },
		"stop":          func(l *agentLoop) { l.acceptControl(bufferedControl("stop", TypeStop)); l.finishStop() },
		"terminate":     func(l *agentLoop) { l.acceptControl(bufferedControl("terminate", TypeTerminate)) },
		"control deadline": func(l *agentLoop) {
			l.acceptControl(bufferedControl("stop", TypeStop))
			l.expireControl(l.executingControl)
		},
	} {
		t.Run(name, func(t *testing.T) {
			l, _ := newUnitLoop()
			l.enqueue(bufferedMsg("owner", "actor:a", 1), false)
			l.handleProviderEvent(providerEvent{kind: eventTurnStarted, op: OpID("op-1"), turnID: "t1"})
			sys := l.sys.(*testSys)
			if len(sys.emits) != 1 || sys.emits[0].Type != "activity.turn.started" {
				t.Fatalf("setup emits=%#v", sys.emits)
			}
			terminalsBefore := len(sys.terms)
			kill(l)
			ended := false
			for _, emit := range sys.emits {
				ended = ended || emit.Type == "activity.turn.ended"
			}
			if !ended {
				t.Fatalf("turn phase left open: emits=%#v", sys.emits)
			}
			// Order is part of the record: replaying the log must show the
			// turn's owner settled before the turn is declared over. (A control
			// verb's own receipt is a different request and may land after.)
			if len(sys.terms) <= terminalsBefore {
				t.Fatalf("turn died without settling its owner: terms=%#v", sys.terms)
			}
			ownerAt, endedAt := -1, -1
			for i, entry := range sys.order {
				switch entry {
				case "fail:owner", "reply:owner":
					ownerAt = i
				case "emit:activity.turn.ended":
					endedAt = i
				}
			}
			if ownerAt < 0 || endedAt < 0 || ownerAt > endedAt {
				t.Fatalf("owner terminal must precede turn.ended: order=%v", sys.order)
			}
		})
	}
}

// A stop removes the waiter on purpose, so the answer that lands afterwards is
// not a dropped result and must not be reported as one — otherwise every
// successful stop pollutes the observation stream.
func TestStopFollowedByAnswerPublishesNoOrphanObservation(t *testing.T) {
	l, e := newUnitLoop()
	l.enqueue(bufferedMsg("owner", "actor:a", 1), false)
	l.handleProviderEvent(providerEvent{kind: eventTurnStarted, op: OpID("op-1"), turnID: "t1"})
	l.acceptControl(bufferedControl("stop", TypeStop))
	l.finishExecutingControl(providerEvent{op: e.interrupts[0], verdict: ControlAccepted})
	l.handleProviderEvent(providerEvent{kind: eventTurnEnded, turnID: "t1", status: TurnStatusInterrupted})
	if obs := l.sys.(*testSys).obs; len(obs) != 0 {
		t.Fatalf("a deliberate stop reported a dropped result: %v", obs)
	}
}

func TestOrphanFinalPublishesLoudObservation(t *testing.T) {
	l, _ := newUnitLoop()
	l.state, l.turnID, l.turnIndex = stateTurnActive, "turn-orphan", 3
	l.lastOwner = bufferedMsg("closed-owner", "actor:a", 1)
	l.lastOwner.closed = true
	l.result = &turnResult{status: TurnStatusOK, text: "unowned answer"}
	l.settleTurn()
	obs := l.sys.(*testSys).obs
	if len(obs) != 1 || obs[0] != ObsOrphanTurnResult {
		t.Fatalf("orphan observations=%v", obs)
	}
}
