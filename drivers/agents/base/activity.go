package base

import (
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	processKindTurn  = "turn"
	processKindStage = "stage"
	processKindTool  = "tool"
	processStarted   = "started"
	processEnded     = "ended"
	toolCompleted    = "completed"
	toolFailed       = "failed"
)

// progressProcess is the only Agent Base → ledger projection for runtime
// process observations. Provider/runtime events stay internal; on the wire a
// process belongs to the request this Agent is serving, so it is a provisional
// response from the serving Actor rather than a free-standing event.
func (l *agentLoop) progressProcess(process map[string]any) {
	t := l.state.Turn
	if t == nil || t.Owner == "" {
		return
	}
	row := l.state.Requests[t.Owner]
	if row == nil {
		return
	}
	value := map[string]any{
		"turn_id":  t.ID,
		"controls": l.processingControls(),
	}
	if process != nil {
		value["process"] = process
	}
	l.exec.progress(string(row.ID), message.StatusProcessing, value)
}

func (l *agentLoop) progressTurnStarted() {
	t := l.state.Turn
	if t == nil {
		return
	}
	l.progressProcess(map[string]any{
		"kind":       processKindTurn,
		"phase":      processStarted,
		"turn_index": t.Serial,
	})
}

func (l *agentLoop) progressStage(kind, text string) {
	if kind == "" {
		return
	}
	l.progressProcess(map[string]any{
		"kind":  processKindStage,
		"stage": kind,
		"text":  text,
	})
}

func (l *agentLoop) progressTool(v toolEvent) {
	t := l.state.Turn
	if v.CallID == "" || v.Name == "" || t == nil {
		return
	}
	process := map[string]any{
		"kind":         processKindTool,
		"phase":        v.Phase,
		"turn_index":   t.Serial,
		"tool_call_id": v.CallID,
		"tool":         v.Name,
	}
	switch v.Phase {
	case processStarted:
		if len(v.Input) != 0 {
			process["input"] = v.Input
		}
	case processEnded:
		outcome := v.Status
		if outcome == "" {
			outcome = toolCompleted
		}
		if outcome != toolCompleted && outcome != toolFailed {
			l.logger.Error("agent invalid tool outcome", "outcome", outcome)
			return
		}
		process["outcome"] = outcome
		if v.Detail != "" {
			process["detail"] = v.Detail
		}
		if len(v.Output) != 0 {
			process["output"] = l.prepareToolOutput(v.Output)
		}
	default:
		l.logger.Error("agent invalid tool phase", "phase", v.Phase)
		return
	}
	l.progressProcess(process)
}
