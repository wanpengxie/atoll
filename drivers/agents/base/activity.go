package base

import (
	"fmt"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func (l *agentLoop) emit(typ registry.ActivityType, payload any) {
	t := l.book.turn
	if t == nil || t.anchor == nil {
		return
	}
	spec, err := behavior.EventSpecJSON(string(typ), payload)
	if err != nil {
		return
	}
	spec.Audience = message.Audience{}
	spec.Visibility = message.VisibilityPublic
	spec.ParentID = t.anchor.msg.ID
	spec.CorrelationID = t.anchor.msg.CorrelationID
	if spec.CorrelationID == "" {
		spec.CorrelationID = t.anchor.msg.ID
	}
	_, err = l.sys.Emit(spec)
	l.logError("agent activity write failed", err)
}
func (l *agentLoop) emitTurnStarted() {
	t := l.book.turn
	if t != nil {
		l.emit(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{TurnIndex: int(t.seq), Status: registry.ActivityStartedStatus})
	}
}
func (l *agentLoop) emitTurnEnded(s TurnStatus) {
	t := l.book.turn
	if t != nil {
		l.emit(registry.ActivityTurnEnded, registry.ActivityTurnEndedPayload{TurnIndex: int(t.seq), Status: string(s)})
	}
}
func (l *agentLoop) emitTool(v ToolEvent) {
	if v.CallID == "" || v.Name == "" {
		return
	}
	t := l.book.turn
	if t == nil {
		return
	}
	if v.Phase == "started" {
		l.emit(registry.ActivityToolStarted, registry.ActivityToolStartedPayload{TurnIndex: int(t.seq), ToolCallID: v.CallID, Tool: v.Name, Status: registry.ActivityStartedStatus})
		return
	}
	status := v.Status
	if status == "" {
		status = registry.ActivityToolEndedStatusCompleted
	}
	if !registry.IsActivityToolEndedStatus(status) {
		l.logError("agent invalid tool status", fmt.Errorf("%s", status))
		return
	}
	l.emit(registry.ActivityToolEnded, registry.ActivityToolEndedPayload{TurnIndex: int(t.seq), ToolCallID: v.CallID, Tool: v.Name, Status: status, Detail: v.Detail})
}
