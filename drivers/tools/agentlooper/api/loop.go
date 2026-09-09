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
	TypeReset   = "loop.reset"
	TypeSync    = "loop.sync"
	TypeRename  = "loop.rename"
	TypeInspect = "loop.inspect"
	TypeReport  = "loop.report"
)

const (
	TypeSessionOpened  = "session.opened"
	TypeSessionCompact = "session.compact"
	TypeSessionSync    = "session.sync"
	TypeSessionReset   = "session.reset"
	TypeSessionRename  = "session.rename"
)

// MaxControlBytes bounds the accepted input set for one live execution.
const MaxControlBytes = 4 << 20

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
	SessionID          string            `json:"session_id,omitempty"`
	TurnID             string            `json:"turn_id,omitempty"`
	Open               *OpenRequest      `json:"open,omitempty"`
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
	Prompt             string            `json:"prompt,omitempty"`
	Model              string            `json:"model,omitempty"`
	Effort             string            `json:"effort,omitempty"`
	MaxTurns           int               `json:"max_turns,omitempty"`
	Tools              *[]ToolBinding    `json:"tools,omitempty"`
	ToolResultMaxLines int               `json:"tool_result_max_lines,omitempty"`
	ToolResultMaxBytes int               `json:"tool_result_max_bytes,omitempty"`
	ToolImageMaxBytes  int               `json:"tool_image_max_bytes,omitempty"`
	ContextWindow      int               `json:"context_window,omitempty"`
	ReserveTokens      int               `json:"reserve_tokens,omitempty"`
	KeepRecentTokens   int               `json:"keep_recent_tokens,omitempty"`
	CompactModel       string            `json:"compact_model,omitempty"`
}

type startWire struct {
	RootSession string       `json:"session,omitempty"`
	SessionID   string       `json:"session_id"`
	TurnID      string       `json:"turn_id"`
	Open        *OpenRequest `json:"open,omitempty"`
	Inputs      []string     `json:"inputs"`
	Selection   struct {
		Model  string `json:"model,omitempty"`
		Effort string `json:"effort,omitempty"`
	} `json:"selection"`
	Prompt string         `json:"prompt,omitempty"`
	Tools  *[]ToolBinding `json:"tools,omitempty"`
	Limits struct {
		ToolTimeoutMS      int64  `json:"tool_timeout_ms,omitempty"`
		ExecutionTimeoutMS int64  `json:"execution_timeout_ms,omitempty"`
		MaxTurns           int    `json:"max_turns,omitempty"`
		ToolResultMaxLines int    `json:"tool_result_max_lines,omitempty"`
		ToolResultMaxBytes int    `json:"tool_result_max_bytes,omitempty"`
		ToolImageMaxBytes  int    `json:"tool_image_max_bytes,omitempty"`
		ContextWindow      int    `json:"context_window,omitempty"`
		ReserveTokens      int    `json:"reserve_tokens,omitempty"`
		KeepRecentTokens   int    `json:"keep_recent_tokens,omitempty"`
		CompactModel       string `json:"compact_model,omitempty"`
	} `json:"limits,omitempty"`
}

func (r StartRequest) MarshalJSON() ([]byte, error) {
	var w startWire
	w.RootSession, w.SessionID, w.TurnID, w.Open, w.Prompt, w.Tools = r.SessionID, r.SessionID, r.TurnID, r.Open, r.Prompt, r.Tools
	if w.TurnID == "" {
		w.TurnID = r.AssignmentID
	}
	for _, input := range r.Inputs {
		w.Inputs = append(w.Inputs, input.ID)
	}
	w.Selection.Model = r.Model
	w.Selection.Effort = r.Effort
	w.Limits.ToolTimeoutMS, w.Limits.ExecutionTimeoutMS, w.Limits.MaxTurns = r.ToolTimeoutMS, r.ExecutionTimeoutMS, r.MaxTurns
	w.Limits.ToolResultMaxLines, w.Limits.ToolResultMaxBytes, w.Limits.ToolImageMaxBytes = r.ToolResultMaxLines, r.ToolResultMaxBytes, r.ToolImageMaxBytes
	w.Limits.ContextWindow, w.Limits.ReserveTokens, w.Limits.KeepRecentTokens, w.Limits.CompactModel = r.ContextWindow, r.ReserveTokens, r.KeepRecentTokens, r.CompactModel
	return json.Marshal(w)
}

func (r *StartRequest) UnmarshalJSON(raw []byte) error {
	var w startWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	r.SessionID, r.TurnID, r.AssignmentID, r.Open, r.Prompt, r.Tools = w.SessionID, w.TurnID, w.TurnID, w.Open, w.Prompt, w.Tools
	r.Model = w.Selection.Model
	r.Effort = w.Selection.Effort
	for i, id := range w.Inputs {
		r.Inputs = append(r.Inputs, Input{ID: id, Seq: int64(i + 1)})
	}
	r.ToolTimeoutMS, r.ExecutionTimeoutMS, r.MaxTurns = w.Limits.ToolTimeoutMS, w.Limits.ExecutionTimeoutMS, w.Limits.MaxTurns
	r.ToolResultMaxLines, r.ToolResultMaxBytes, r.ToolImageMaxBytes = w.Limits.ToolResultMaxLines, w.Limits.ToolResultMaxBytes, w.Limits.ToolImageMaxBytes
	r.ContextWindow, r.ReserveTokens, r.KeepRecentTokens, r.CompactModel = w.Limits.ContextWindow, w.Limits.ReserveTokens, w.Limits.KeepRecentTokens, w.Limits.CompactModel
	return nil
}

type BoundaryRef struct {
	Session string `json:"session"`
	At      string `json:"at,omitempty"`
}

type SyncRange struct {
	Session string `json:"session"`
	After   string `json:"after,omitempty"`
	Through string `json:"through"`
}

type OpenRequest struct {
	Base *BoundaryRef `json:"base,omitempty"`
}

type InputRequest struct {
	SessionID    string            `json:"session_id,omitempty"`
	TurnID       string            `json:"turn_id,omitempty"`
	ControlID    string            `json:"control_id,omitempty"`
	Inputs       []Input           `json:"inputs,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	Input        Input             `json:"input,omitempty"`
}

func (r InputRequest) MarshalJSON() ([]byte, error) {
	turn := r.TurnID
	if turn == "" {
		turn = r.AssignmentID
	}
	ids := make([]string, 0, len(r.Inputs))
	for _, input := range r.Inputs {
		ids = append(ids, input.ID)
	}
	if len(ids) == 0 && r.Input.ID != "" {
		ids = append(ids, r.Input.ID)
	}
	return json.Marshal(struct {
		RootSession string   `json:"session,omitempty"`
		SessionID   string   `json:"session_id"`
		TurnID      string   `json:"turn_id"`
		ControlID   string   `json:"control_id,omitempty"`
		Inputs      []string `json:"inputs"`
	}{r.SessionID, r.SessionID, turn, r.ControlID, ids})
}

func (r *InputRequest) UnmarshalJSON(raw []byte) error {
	var wire struct {
		SessionID string   `json:"session_id"`
		TurnID    string   `json:"turn_id"`
		ControlID string   `json:"control_id"`
		Inputs    []string `json:"inputs"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	r.SessionID, r.TurnID, r.AssignmentID, r.ControlID = wire.SessionID, wire.TurnID, wire.TurnID, wire.ControlID
	for i, id := range wire.Inputs {
		r.Inputs = append(r.Inputs, Input{ID: id, Seq: int64(i + 1)})
	}
	return nil
}

type StopRequest struct {
	SessionID    string            `json:"session_id,omitempty"`
	TurnID       string            `json:"turn_id,omitempty"`
	Archive      bool              `json:"archive,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	Reason       string            `json:"reason,omitempty"`
}

func (r StopRequest) MarshalJSON() ([]byte, error) {
	turn := r.TurnID
	if turn == "" {
		turn = r.AssignmentID
	}
	return json.Marshal(struct {
		RootSession string `json:"session,omitempty"`
		SessionID   string `json:"session_id"`
		TurnID      string `json:"turn_id,omitempty"`
		Archive     bool   `json:"archive,omitempty"`
		Reason      string `json:"reason,omitempty"`
	}{r.SessionID, r.SessionID, turn, r.Archive, r.Reason})
}

func (r *StopRequest) UnmarshalJSON(raw []byte) error {
	var wire struct {
		SessionID string `json:"session_id"`
		TurnID    string `json:"turn_id"`
		Archive   bool   `json:"archive"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	r.SessionID, r.TurnID, r.AssignmentID, r.Archive, r.Reason = wire.SessionID, wire.TurnID, wire.TurnID, wire.Archive, wire.Reason
	return nil
}

type InspectRequest struct {
	SessionID    string            `json:"session_id,omitempty"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id,omitempty"`
}

type ReportRequest struct {
	SessionID       string            `json:"session_id,omitempty"`
	TurnID          string            `json:"turn_id,omitempty"`
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

func (r ReportRequest) MarshalJSON() ([]byte, error) {
	turn := r.TurnID
	if turn == "" {
		turn = r.AssignmentID
	}
	return json.Marshal(struct {
		SessionID string `json:"session_id"`
		TurnID    string `json:"turn_id"`
		State     string `json:"state"`
	}{r.SessionID, turn, r.State})
}

func (r *ReportRequest) UnmarshalJSON(raw []byte) error {
	var wire struct {
		SessionID string `json:"session_id"`
		TurnID    string `json:"turn_id"`
		State     string `json:"state"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	r.SessionID, r.TurnID, r.AssignmentID, r.State = wire.SessionID, wire.TurnID, wire.TurnID, wire.State
	return nil
}

type ResetRequest struct {
	SessionID string `json:"session_id"`
}
type RenameRequest struct {
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}
type SyncRequest struct {
	SessionID string    `json:"session_id"`
	From      SyncRange `json:"from"`
}

type Opened struct {
	SessionID string       `json:"session_id"`
	Name      string       `json:"name,omitempty"`
	Base      *BoundaryRef `json:"base,omitempty"`
}
type Compact struct {
	SessionID    string            `json:"session_id"`
	Context      []json.RawMessage `json:"context"`
	Refs         []string          `json:"refs,omitempty"`
	TokensBefore int               `json:"tokens_before,omitempty"`
	Size         ContextSize       `json:"size,omitempty"`
}
type ContextSize struct {
	Before int `json:"before"`
	After  int `json:"after"`
}
type Synced struct {
	SessionID string `json:"session_id"`
	From      struct {
		Session string `json:"session"`
		After   string `json:"after,omitempty"`
		Through string `json:"through"`
	} `json:"from"`
	Context      []json.RawMessage `json:"context"`
	TokensBefore int               `json:"tokens_before,omitempty"`
}

// ControlResult records execution admission, not model consumption. It travels
// in both the control response and terminal report to tolerate reply reordering.
type ControlResult struct {
	ControlID   string  `json:"control_id"`
	Disposition string  `json:"disposition"`
	Inputs      []Input `json:"inputs,omitempty"`
}
