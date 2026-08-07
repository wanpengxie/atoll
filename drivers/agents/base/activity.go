package base

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const ObsOrphanTurnResult actorrt.ObsKind = "agentbase.orphan_turn_result"

func (l *agentLoop) activityAnchor() *requestItem {
	if l.active != nil && !l.active.closed {
		return l.active
	}
	return l.lastOwner
}

// publishOrphanTurnResult reports a final answer that arrived with nobody left
// to receive it. "Nobody left" only counts as a loss when the workspace lost
// its owner to a RACE; a stop or terminate removed the waiter on purpose, and
// the answer arriving afterwards is the expected shape of that order, not a
// dropped result.
func (l *agentLoop) publishOrphanTurnResult(result *turnResult) {
	if result == nil || l.lastOwner == nil || l.ownerDroppedByControl {
		return
	}
	detail, _ := json.Marshal(map[string]any{
		"turn_id": l.turnID, "turn_index": l.turnIndex, "status": result.status,
		"has_final_text": result.text != "", "has_error": result.err != "",
	})
	_ = l.sys.PublishObs(ObsOrphanTurnResult, detail)
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

// closeTurnPhase publishes the closing phase marker for a turn that ends
// WITHOUT a provider verdict — a lost provider, an escalated control deadline,
// a stop. A started turn's phase boundary must close on every path it can die
// on, or the channel log shows a turn that began and never ended. It must run
// before the owners are cleared, since the anchor comes from them.
func (l *agentLoop) closeTurnPhase(status TurnStatus) {
	if l.turnID == "" {
		return
	}
	l.logActivityError(registry.ActivityTurnEnded, l.emitTurnEnded(status))
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
