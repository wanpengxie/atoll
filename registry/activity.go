package registry

// ActivityType is a composition-level durable phase-event type. The registry below is
// the single vocabulary source for producers.
type ActivityType string

const (
	ActivityTurnStarted ActivityType = "activity.turn.started"
	ActivityTurnEnded   ActivityType = "activity.turn.ended"
	ActivityToolStarted ActivityType = "activity.tool.started"
	ActivityToolEnded   ActivityType = "activity.tool.ended"
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
	TurnIndex int    `json:"turn_index"`
	Status    string `json:"status"`
}

type ActivityToolStartedPayload struct {
	TurnIndex  int    `json:"turn_index"`
	ToolCallID string `json:"tool_call_id"`
	Tool       string `json:"tool"`
	Status     string `json:"status"`
}

type ActivityToolEndedPayload struct {
	TurnIndex  int    `json:"turn_index"`
	ToolCallID string `json:"tool_call_id"`
	Tool       string `json:"tool"`
	Status     string `json:"status"`
	Detail     string `json:"detail,omitempty"`
}
