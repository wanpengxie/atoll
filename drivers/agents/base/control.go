package base

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

const controlActionDeadline = 45 * time.Second

func (l *agentLoop) acceptControl(item *requestItem) {
	slot := &controlSlot{kind: item.msg.Type, item: item}
	l.armSlotDeadline(slot)
	if l.pendingControl != nil {
		l.closeSlot(l.pendingControl)
		l.reply(l.pendingControl.item, map[string]any{"superseded_by": item.msg.ID})
	}
	l.pendingControl = slot
	l.maybeRunControl()
}

// armSlotDeadline starts the slot's enforcement clock at enslotment: the
// bound is "a control verb reaches a terminal within T of being accepted" —
// acceptance is part of the bound's definition, so a slot parked pending
// (e.g. behind a starting turn that never reports) expires on the same clock
// as one whose RPC hangs.
func (l *agentLoop) armSlotDeadline(slot *controlSlot) {
	slot.timer = time.AfterFunc(controlActionDeadline, func() {
		select {
		case l.controlExpiry <- slot:
		case <-l.sys.Life().Done():
		}
	})
}

func (l *agentLoop) closeSlot(slot *controlSlot) {
	if slot != nil && slot.timer != nil {
		slot.timer.Stop()
		slot.timer = nil
	}
}

func (l *agentLoop) maybeRunControl() {
	if l.pendingControl == nil || l.executingControl != nil || l.settling {
		return
	}
	// A turn that is starting has no authoritative id yet, and the log has no
	// turn.started row for it. EVERY control verb waits out that window — the
	// slot stays pending until TurnStarted or TurnRejected puts the workspace
	// in a state that can be acted on and recorded. (Both of those paths call
	// back here; a provider that never reports is bounded by the slot's own
	// deadline.) Waiting also keeps the window supersedable: a later control
	// can still replace a pending one, which an early-executing one forbids.
	if l.state == stateStarting {
		return
	}
	slot := l.pendingControl
	l.pendingControl = nil
	l.executingControl = slot
	switch slot.kind {
	case TypeStop:
		l.clearWork("stop")
		l.reply(slot.item, map[string]any{"stopped": true})
		if l.state == stateIdle {
			l.finishStop()
		} else {
			l.submitStopInterrupt(slot)
		}
	case TypeTerminate:
		l.cancelCommittedAndActive("terminate")
		l.closeTurnPhase(TurnStatusInterrupted)
		slot.phase = "terminating"
		l.submitTerminate(slot)
	case TypeRestart:
		l.cancelCommittedAndActive("restart")
		l.closeTurnPhase(TurnStatusInterrupted)
		slot.phase = "terminating"
		l.submitTerminate(slot)
	case TypeInterrupt:
		if l.state == stateIdle {
			l.finishIdleInterrupt(slot)
			return
		}
		slot.op = l.opID()
		l.state = stateInterrupting
		if err := l.eng.Interrupt(slot.op); err != nil {
			l.closeSlot(slot)
			l.fail(slot.item, errorProviderCrash, err.Error())
			l.executingControl = nil
			l.state = stateTurnActive
		}
	}
}

func (l *agentLoop) finishIdleInterrupt(slot *controlSlot) {
	l.closeSlot(slot)
	if controlHasContent(slot.item) {
		l.executingControl = nil
		l.enqueue(slot.item, false)
	} else {
		l.reply(slot.item, map[string]any{"interrupted": ""})
		l.executingControl = nil
		l.maybeRunControl()
		l.startNext()
	}
}

func controlHasContent(item *requestItem) bool {
	if item == nil {
		return false
	}
	var p struct {
		Text string `json:"text"`
	}
	return json.Unmarshal(item.msg.Payload, &p) == nil && strings.TrimSpace(p.Text) != ""
}

func (l *agentLoop) finishExecutingControl(e providerEvent) {
	slot := l.executingControl
	if slot == nil || slot.op != e.op {
		return
	}
	switch slot.kind {
	case TypeTerminate:
		l.closeSlot(slot)
		if e.verdict == ControlAccepted {
			l.reply(slot.item, map[string]any{"terminated": true})
		} else {
			l.fail(slot.item, errorProviderCrash, e.detail)
		}
		l.executingControl = nil
		l.state, l.turnID, l.result, l.settling = stateIdle, "", nil, false
		l.maybeRunControl()
		l.startNext()
	case TypeRestart:
		if slot.phase == "terminating" {
			if e.verdict != ControlAccepted {
				l.closeSlot(slot)
				l.fail(slot.item, errorProviderCrash, e.detail)
				l.executingControl = nil
				return
			}
			slot.phase = "ensuring"
			slot.op = l.opID()
			l.state, l.turnID, l.result, l.settling = stateIdle, "", nil, false
			if err := l.eng.EnsureAlive(slot.op); err != nil {
				l.closeSlot(slot)
				l.fail(slot.item, errorProviderCrash, err.Error())
				l.executingControl = nil
			}
			return
		}
		l.closeSlot(slot)
		if e.verdict == ControlAccepted {
			l.reply(slot.item, map[string]any{"restarted": true})
		} else {
			l.fail(slot.item, errorProviderCrash, e.detail)
		}
		l.executingControl = nil
		l.maybeRunControl()
		l.startNext()
	case TypeInterrupt:
		switch e.verdict {
		case ControlAccepted:
			slot.rpcDone = true
			// The slot remains executing until TurnEnded closes the interrupted
			// workspace (its deadline keeps running across that wait). An
			// executing physical action is never superseded.
			l.maybeSettle()
		case ControlNoActiveTurn:
			slot.rpcDone = true
			// The provider no longer considers the turn active, but base still
			// owns its request and phase until TurnEnded supplies the authoritative
			// terminal. Dropping to idle here would disarm every backstop and make
			// a later TurnEnded unmatchable, leaving the active request open forever.
			// Keep the slot/window alive; a missing TurnEnded is harvested by the
			// slot deadline as a provider failure.
			l.maybeSettle()
		default:
			l.closeSlot(slot)
			l.fail(slot.item, errorProviderCrash, e.detail)
			l.executingControl = nil
			if l.turnID == "" {
				l.state = stateIdle
			} else {
				l.state = stateTurnActive
			}
			l.maybeRunControl()
		}
	case TypeStop:
		switch e.verdict {
		case ControlAccepted:
			slot.rpcDone = true
			l.maybeSettle()
		case ControlNoActiveTurn:
			slot.rpcDone = true
			if l.result != nil {
				l.maybeSettle()
			} else {
				l.finishStop()
			}
		default:
			// A transport failure leaves the provider's turn state unknowable.
			// This is failure recovery, not normal stop semantics: retire the
			// connection before allowing new work.
			_ = l.eng.Terminate()
			l.finishStop()
		}
	}
}

// submitTerminate runs inline: Engine.Terminate is contractually non-blocking
// (physical reaping is the engine's async internals), so the arbiter loop can
// execute the action in the same instant it decides it — there is no deferred
// killer that could land on a generation spawned after the decision.
func (l *agentLoop) submitTerminate(slot *controlSlot) {
	slot.op = l.opID()
	verdict, detail := ControlAccepted, ""
	if err := l.eng.Terminate(); err != nil {
		verdict, detail = ControlRPCError, err.Error()
	}
	l.finishExecutingControl(providerEvent{kind: eventControlDone, op: slot.op, verdict: verdict, detail: detail})
}

func (l *agentLoop) submitStopInterrupt(slot *controlSlot) {
	if slot.op == "" {
		slot.op = l.opID()
	}
	l.state = stateInterrupting
	if err := l.eng.Interrupt(slot.op); err != nil {
		slog.Error("agent stop interrupt submission failed", "actor", l.sys.Self(), "error", err)
		_ = l.eng.Terminate()
		l.finishStop()
	}
}

func (l *agentLoop) finishStop() {
	l.closeSlot(l.executingControl)
	l.executingControl = nil
	l.closeTurnPhase(TurnStatusInterrupted)
	l.result, l.active, l.turnID, l.settling, l.state = nil, nil, "", false, stateIdle
	l.maybeRunControl()
	l.startNext()
}

// expireControl fires when a slot's enforcement clock runs out before the slot
// reached a terminal — pending (parked behind a window that never closed) and
// executing (a physical action that hung) expire on the same clock. The
// escalation is uniform: reap the connection (the one action that always
// bounds "stop"), settle the work accounts by the verb's own semantics, and
// close the slot loudly.
func (l *agentLoop) expireControl(slot *controlSlot) {
	if slot == nil || (slot != l.executingControl && slot != l.pendingControl) {
		return // slot already terminal; stale timer fire
	}
	l.closeSlot(slot)
	if slot == l.pendingControl {
		l.pendingControl = nil
	} else {
		l.executingControl = nil
	}
	detail := slot.kind + " control deadline exceeded"
	status := TurnStatusFailed
	if slot.kind == TypeInterrupt || slot.kind == TypeStop {
		status = TurnStatusInterrupted
	}
	_ = l.eng.Terminate()
	if slot.kind == TypeInterrupt {
		if l.active != nil {
			l.fail(l.active, errorInterrupted, detail)
		}
		for id, pending := range l.committing {
			if pending.item != nil {
				l.fail(pending.item, errorInterrupted, detail)
			}
			delete(l.committing, id)
		}
		l.active = nil
	} else if slot.kind == TypeStop {
		l.clearWork(detail)
	} else {
		l.cancelCommittedAndActive(detail)
	}
	if !slot.item.closed {
		l.fail(slot.item, errorProviderCrash, detail)
	}
	// Phase marker last: the log order is terminals first, turn.ended after.
	l.closeTurnPhase(status)
	l.result, l.turnID, l.settling, l.state = nil, "", false, stateIdle
	l.maybeRunControl()
	l.startNext()
}

func (l *agentLoop) cancelCommittedAndActive(detail string) {
	if l.active != nil {
		l.fail(l.active, errorCancelled, detail)
	}
	for op, pending := range l.committing {
		if pending.item != nil {
			l.fail(pending.item, errorCancelled, detail)
		}
		delete(l.committing, op)
	}
	l.active = nil
	l.ownerDroppedByControl = true
	// lastOwner deliberately survives: it is the TURN's phase anchor, not a
	// workspace slot. Clearing work must not destroy the ability to publish
	// the turn's closing marker; the next turn overwrites it at TurnStarted.
}

func (l *agentLoop) clearWork(detail string) {
	l.cancelCommittedAndActive(detail)
	for _, item := range l.buffer.items {
		l.fail(item, errorCancelled, detail)
	}
	l.buffer.items = nil
	l.buffer.bytes = 0
}
