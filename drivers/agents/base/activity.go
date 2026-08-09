package base

import (
	"fmt"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func (l *agentLoop) emit(typ registry.ActivityType, payload any) {
	t := l.state.Turn
	if t == nil || t.AnchorParent == "" {
		return
	}
	spec, err := behavior.EventSpecJSON(string(typ), payload)
	if err != nil {
		return
	}
	spec = emptyAudiencePublic(spec)
	spec.ParentID = message.ID(t.AnchorParent)
	spec.CorrelationID = message.ID(t.AnchorCorrelation)
	if spec.CorrelationID == "" {
		spec.CorrelationID = spec.ParentID
	}
	l.exec.emit(spec)
}
func (l *agentLoop) emitTurnStarted() {
	if t := l.state.Turn; t != nil {
		l.emit(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{TurnIndex: int(t.Serial), Status: registry.ActivityStartedStatus})
	}
}
func (l *agentLoop) emitTurnEnded(status string) {
	if t := l.state.Turn; t != nil {
		l.emit(registry.ActivityTurnEnded, registry.ActivityTurnEndedPayload{TurnIndex: int(t.Serial), Status: status})
	}
}
func (l *agentLoop) emitTool(v toolEvent) {
	if v.CallID == "" || v.Name == "" || l.state.Turn == nil {
		return
	}
	if v.Phase == "started" {
		l.emit(registry.ActivityToolStarted, registry.ActivityToolStartedPayload{TurnIndex: int(l.state.Turn.Serial), ToolCallID: v.CallID, Tool: v.Name, Status: registry.ActivityStartedStatus})
		return
	}
	status := v.Status
	if status == "" {
		status = registry.ActivityToolEndedStatusCompleted
	}
	if !registry.IsActivityToolEndedStatus(status) {
		l.logger.Error("agent invalid tool status", "status", fmt.Sprint(status))
		return
	}
	l.emit(registry.ActivityToolEnded, registry.ActivityToolEndedPayload{TurnIndex: int(l.state.Turn.Serial), ToolCallID: v.CallID, Tool: v.Name, Status: status, Detail: v.Detail})
}
