package api

import (
	"encoding/json"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	TypeStart   = "loop.start"
	TypeInput   = "loop.input"
	TypeStop    = "loop.stop"
	TypeInspect = "loop.inspect"
	TypeReport  = "loop.report"
)

// MaxHistoryBytes keeps one internal report and recovery snapshot below the
// transport ceiling with room for its envelope and work metadata.
const MaxHistoryBytes = 4 << 20

type Input struct {
	ID            string             `json:"input_id"`
	Seq           int64              `json:"seq"`
	Text          string             `json:"text"`
	Attachments   []json.RawMessage  `json:"attachments,omitempty"`
	CallerChannel channel.ID         `json:"caller_channel,omitempty"`
	CallerActor   actor.ActorID      `json:"caller_actor,omitempty"`
	Origin        *agentproto.Origin `json:"origin,omitempty"`
}

// ToolBinding is one model-visible name bound to one actor request word.
// The model chooses only Name; Actor and Word remain fixed for the episode.
type ToolBinding struct {
	Name  string `json:"name"`
	Actor string `json:"actor"`
	Word  string `json:"word"`
}

type StartRequest struct {
	ViewID             string            `json:"view_id,omitempty"`
	ContextVersion     int64             `json:"context_version,omitempty"`
	ToolTimeoutMS      int64             `json:"tool_timeout_ms,omitempty"`
	ExecutionTimeoutMS int64             `json:"execution_timeout_ms,omitempty"`
	WorkID             agentproto.WorkID `json:"work_id"`
	AssignmentID       string            `json:"assignment_id"`
	ControllerActor    string            `json:"controller_actor"`
	Inputs             []Input           `json:"inputs"`
	Prior              []json.RawMessage `json:"prior,omitempty"`
	ContextActor       string            `json:"context_actor"`
	LLMActor           string            `json:"llm_actor"`
	WorkspaceActor     string            `json:"workspace_actor,omitempty"`
	HostActor          string            `json:"host_actor,omitempty"`
	Model              string            `json:"model,omitempty"`
	MaxTurns           int               `json:"max_turns,omitempty"`
	Tools              *[]ToolBinding    `json:"tools,omitempty"`
	ToolResultMaxLines int               `json:"tool_result_max_lines,omitempty"`
	ToolResultMaxBytes int               `json:"tool_result_max_bytes,omitempty"`
	ToolImageMaxBytes  int               `json:"tool_image_max_bytes,omitempty"`
}

type InputRequest struct {
	ViewID       string            `json:"view_id,omitempty"`
	ControlID    string            `json:"control_id,omitempty"`
	Inputs       []Input           `json:"inputs,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	Input        Input             `json:"input,omitempty"`
}

type StopRequest struct {
	ViewID       string            `json:"view_id,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	Reason       string            `json:"reason,omitempty"`
}

type InspectRequest struct {
	ViewID       string            `json:"view_id,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id,omitempty"`
}

type ReportRequest struct {
	ViewID          string            `json:"view_id,omitempty"`
	ContextVersion  int64             `json:"context_version,omitempty"`
	Controls        []ControlResult   `json:"controls,omitempty"`
	WorkID          agentproto.WorkID `json:"work_id"`
	AssignmentID    string            `json:"assignment_id"`
	State           string            `json:"state"`
	ConsumedThrough int64             `json:"consumed_through,omitempty"`
	Result          json.RawMessage   `json:"result,omitempty"`
	ErrorCode       string            `json:"error_code,omitempty"`
	Detail          string            `json:"detail,omitempty"`
	ExecutionState  string            `json:"execution_state,omitempty"`
	History         []json.RawMessage `json:"history,omitempty"`
}

// ControlResult records execution admission, not model consumption. It travels
// in both the control response and terminal report to tolerate reply reordering.
type ControlResult struct {
	ControlID   string  `json:"control_id"`
	Disposition string  `json:"disposition"`
	Inputs      []Input `json:"inputs,omitempty"`
}
