package base

import (
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func (l *agentLoop) activityAnchor() *requestItem {
	if l.active != nil && !l.active.closed {
		return l.active
	}
	return l.lastOwner
}

func (l *agentLoop) emitActivity(typ registry.ActivityType, payload any) error {
	anchor := l.activityAnchor()
	if anchor == nil {
		return nil
	}
	spec, err := behavior.EventSpecJSON(string(typ), payload)
	if err != nil {
		return err
	}
	// Explicit non-nil empty audience is part of the wire contract.
	spec.Audience = message.Audience{}
	spec.Visibility = message.VisibilityPublic
	spec.ParentID = anchor.msg.ID
	spec.CorrelationID = anchor.trigger.CorrelationID
	_, err = l.sys.Emit(spec)
	return err
}

func (l *agentLoop) emitTurnStarted() error {
	return l.emitActivity(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{
		TurnIndex: l.turnIndex, Status: registry.ActivityStartedStatus,
	})
}

func (l *agentLoop) emitTurnEnded(status TurnStatus) error {
	if !registry.IsActivityTurnEndedStatus(string(status)) {
		return fmt.Errorf("agent/base: invalid turn-ended status %q", status)
	}
	return l.emitActivity(registry.ActivityTurnEnded, registry.ActivityTurnEndedPayload{
		TurnIndex: l.turnIndex, Status: string(status),
	})
}

func (l *agentLoop) emitTool(e providerEvent) error {
	if e.callID == "" || e.name == "" {
		return errors.New("agent/base: tool activity requires call id and name")
	}
	switch e.phase {
	case "started":
		return l.emitActivity(registry.ActivityToolStarted, registry.ActivityToolStartedPayload{
			TurnIndex: l.turnIndex, ToolCallID: e.callID, Tool: e.name, Status: registry.ActivityStartedStatus,
		})
	case "ended":
		status := e.toolStatus
		if status == "" {
			status = registry.ActivityToolEndedStatusCompleted
		}
		if !registry.IsActivityToolEndedStatus(status) {
			return fmt.Errorf("agent/base: invalid tool-ended status %q", status)
		}
		return l.emitActivity(registry.ActivityToolEnded, registry.ActivityToolEndedPayload{
			TurnIndex: l.turnIndex, ToolCallID: e.callID, Tool: e.name, Status: status, Detail: e.detail,
		})
	default:
		return nil
	}
}
