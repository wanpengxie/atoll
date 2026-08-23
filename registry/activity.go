package registry

import "encoding/json"

// ActivityType is a composition-level durable phase-event type. The registry below is
// the single vocabulary source for producers.
type ActivityType string

const (
	ActivityTurnStarted ActivityType = "agent.turn.started"
	ActivityTurnEnded   ActivityType = "agent.turn.ended"
	ActivityToolStarted ActivityType = "agent.tool.started"
	ActivityToolEnded   ActivityType = "agent.tool.ended"
)

const (
	ActivityStartedStatus = "started"

	ActivityTurnEndedStatusOK          = "ok"
	ActivityTurnEndedStatusFailed      = "failed"
	ActivityTurnEndedStatusInterrupted = "interrupted"

	ActivityToolEndedStatusCompleted = "completed"
	ActivityToolEndedStatusFailed    = "failed"
)

func IsActivityTurnEndedStatus(status string) bool {
	return status == ActivityTurnEndedStatusOK || status == ActivityTurnEndedStatusFailed || status == ActivityTurnEndedStatusInterrupted
}

func IsActivityToolEndedStatus(status string) bool {
	return status == ActivityToolEndedStatusCompleted || status == ActivityToolEndedStatusFailed
}

type ActivityTurnStartedPayload struct {
	TurnIndex int    `json:"turn_index"`
	Status    string `json:"status"`
}

type ActivityTurnEndedPayload struct {
	TurnIndex int               `json:"turn_index"`
	Status    string            `json:"status"`
	Usage     *TurnUsagePayload `json:"usage,omitempty"`
}

type TurnUsagePayload struct {
	ContextTokens int64  `json:"context_tokens"`
	ContextWindow int64  `json:"context_window"`
	Model         string `json:"model"`
	Effort        string `json:"effort"`
}

type ActivityToolStartedPayload struct {
	TurnIndex  int             `json:"turn_index"`
	ToolCallID string          `json:"tool_call_id"`
	Tool       string          `json:"tool"`
	Status     string          `json:"status"`
	Input      json.RawMessage `json:"input,omitempty"`
}

type ActivityToolEndedPayload struct {
	TurnIndex  int             `json:"turn_index"`
	ToolCallID string          `json:"tool_call_id"`
	Tool       string          `json:"tool"`
	Status     string          `json:"status"`
	Detail     string          `json:"detail,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
}
