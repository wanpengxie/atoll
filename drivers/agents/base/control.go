package base

import (
	"encoding/json"
	"strings"
	"time"
)

const controlActionDeadline = 45 * time.Second

func (l *agentLoop) acceptControl(item *requestItem) {
	slot := &controlSlot{kind: item.msg.Type, item: item}
	if l.pendingControl != nil {
		l.reply(l.pendingControl.item, map[string]any{"superseded_by": item.msg.ID})
	}
	l.pendingControl = slot
	l.maybeRunControl()
}

func (l *agentLoop) maybeRunControl() {
	if l.pendingControl == nil || l.executingControl != nil || l.settling {
		return
	}
	// Interrupt needs a provider turn id. Lifecycle controls can fence a
	// start-in-flight immediately through Terminate and must not wait for a
	// potentially wedged turn/start RPC to report TurnStarted.
	if l.state == stateStarting && l.pendingControl.kind == TypeInterrupt {
		return
	}
	slot := l.pendingControl
	l.pendingControl = nil
	l.executingControl = slot
	switch slot.kind {
	case TypeStop:
		l.clearWork("stop")
		// Stop is allowed to reply mechanically, but the old provider turn must
		// be fenced before another request can start. Terminate synchronously
		// retires the connection while preserving the resume seed for lazy boot.
		if err := l.eng.Terminate(); err != nil {
			l.fail(slot.item, errorProviderCrash, err.Error())
		} else {
			l.reply(slot.item, map[string]any{"stopped": true})
		}
		l.executingControl = nil
		l.state, l.turnID, l.result, l.settling = stateIdle, "", nil, false
		l.maybeRunControl()
		l.startNext()
	case TypeTerminate:
		l.cancelCommittedAndActive("terminate")
		if err := l.eng.Terminate(); err != nil {
			l.fail(slot.item, errorProviderCrash, err.Error())
		} else {
			l.reply(slot.item, map[string]any{"terminated": true})
		}
		l.executingControl = nil
		l.state, l.turnID, l.result, l.settling = stateIdle, "", nil, false
		l.maybeRunControl()
		l.startNext()
	case TypeRestart:
		l.cancelCommittedAndActive("restart")
		if err := l.eng.Terminate(); err != nil {
			l.fail(slot.item, errorProviderCrash, err.Error())
			l.executingControl = nil
			return
		}
		slot.op = l.opID()
		l.state, l.turnID, l.result, l.settling = stateIdle, "", nil, false
		if err := l.eng.EnsureAlive(slot.op); err != nil {
			l.fail(slot.item, errorProviderCrash, err.Error())
			l.executingControl = nil
		} else {
			l.armControlDeadline(slot.op)
		}
	case TypeInterrupt:
		if l.state == stateIdle {
			l.finishIdleInterrupt(slot)
			return
		}
		slot.op = l.opID()
		l.state = stateInterrupting
		if err := l.eng.Interrupt(slot.op); err != nil {
			l.fail(slot.item, errorProviderCrash, err.Error())
			l.executingControl = nil
			l.state = stateTurnActive
		} else {
			l.armControlDeadline(slot.op)
		}
	}
}

func (l *agentLoop) finishIdleInterrupt(slot *controlSlot) {
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
	if slot.kind != TypeInterrupt || e.verdict != ControlAccepted {
		l.stopControlDeadline()
	}
	switch slot.kind {
	case TypeRestart:
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
			// The slot remains executing until TurnEnded closes the interrupted
			// workspace. An executing physical action is never superseded.
		case ControlNoActiveTurn:
			l.state, l.turnID = stateIdle, ""
			l.finishIdleInterrupt(slot)
		default:
			l.fail(slot.item, errorProviderCrash, e.detail)
			l.executingControl = nil
			if l.turnID == "" {
				l.state = stateIdle
			} else {
				l.state = stateTurnActive
			}
			l.maybeRunControl()
		}
	}
}

func (l *agentLoop) armControlDeadline(op OpID) {
	if l.controlExpiry == nil || op == "" {
		return
	}
	l.stopControlDeadline()
	l.controlTimer = time.AfterFunc(controlActionDeadline, func() {
		select {
		case l.controlExpiry <- op:
		case <-l.sys.Life().Done():
		}
	})
}

func (l *agentLoop) stopControlDeadline() {
	if l.controlTimer != nil {
		l.controlTimer.Stop()
		l.controlTimer = nil
	}
}

func (l *agentLoop) expireControl(op OpID) {
	slot := l.executingControl
	if slot == nil || slot.op != op {
		return
	}
	l.stopControlDeadline()
	detail := slot.kind + " control deadline exceeded"
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
	} else if slot.kind == TypeStop {
		l.clearWork(detail)
	} else {
		l.cancelCommittedAndActive(detail)
	}
	l.fail(slot.item, errorProviderCrash, detail)
	l.executingControl = nil
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
	l.lastOwner = nil
}

func (l *agentLoop) clearWork(detail string) {
	l.cancelCommittedAndActive(detail)
	for _, item := range l.buffer.items {
		l.fail(item, errorCancelled, detail)
	}
	l.buffer.items = nil
	l.buffer.bytes = 0
}
