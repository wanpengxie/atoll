package base

import (
	"fmt"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func (l *agentLoop) emit(typ registry.ActivityType, payload any) {
	t := l.state.Turn
	if t == nil || t.AnchorParent == "" {
		return
	}
	// A turn event reports on the request that started the turn. The anchor is
	// that request's id and correlation, held here rather than the envelope
	// because a turn outlives a restart and the envelope does not.
	cause := message.Anchored(message.ID(t.AnchorParent), message.ID(t.AnchorCorrelation))
	spec, err := behavior.EventSpecJSON(cause, string(typ), payload)
	if err != nil {
		return
	}
	l.exec.emit(emptyAudiencePublic(spec))
}
func (l *agentLoop) emitTurnStarted() {
	if t := l.state.Turn; t != nil {
		l.emit(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{TurnIndex: int(t.Serial), Status: registry.ActivityStartedStatus})
	}
}
func (l *agentLoop) emitTurnEnded(status string, usage *runtimeproto.TurnUsage) {
	if t := l.state.Turn; t != nil {
		var payload *registry.TurnUsagePayload
		if usage != nil {
			payload = usagePayload(*usage)
		}
		l.emit(registry.ActivityTurnEnded, registry.ActivityTurnEndedPayload{TurnIndex: int(t.Serial), Status: status, Usage: payload})
	}
}

func usagePayload(usage runtimeproto.TurnUsage) *registry.TurnUsagePayload {
	return &registry.TurnUsagePayload{ContextTokens: usage.ContextTokens, ContextWindow: usage.ContextWindow, Model: usage.Model, Effort: usage.Effort}
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
