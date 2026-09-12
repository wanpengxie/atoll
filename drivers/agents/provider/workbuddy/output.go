package workbuddy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type updateNotice struct {
	SessionID string `json:"sessionId"`
	Update    struct {
		Type    string `json:"sessionUpdate"`
		Content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Used       int64           `json:"used"`
		Size       int64           `json:"size"`
		ToolCallID string          `json:"toolCallId"`
		Title      string          `json:"title"`
		Kind       string          `json:"kind"`
		Status     string          `json:"status"`
		RawInput   json.RawMessage `json:"rawInput"`
		RawOutput  json.RawMessage `json:"rawOutput"`
		Plan       []struct {
			Content string `json:"content"`
			Status  string `json:"status"`
		} `json:"entries"`
		AvailableCommands []struct {
			Name string `json:"name"`
		} `json:"availableCommands"`
	} `json:"update"`
	Meta map[string]any `json:"_meta"`
}

func (w *worker) notification(c *connection, method string, params json.RawMessage) {
	if method == "_codebuddy.ai/models_changed" {
		w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticInfo, Code: "model_catalog_changed", Detail: "WorkBuddy announced a model catalog change; update this actor's configured selections if the new catalog differs"})
		return
	}
	if method != "session/update" {
		return
	}
	var n updateNotice
	if json.Unmarshal(params, &n) != nil {
		return
	}
	w.mu.Lock()
	if w.conn != c || n.SessionID != w.session {
		w.mu.Unlock()
		return
	}
	target := w.target
	active := w.phase == phaseActive
	switch n.Update.Type {
	case "agent_message_chunk":
		if active && n.Update.Content.Type == "text" {
			w.final.WriteString(n.Update.Content.Text)
		}
	case "agent_thought_chunk":
		if active && n.Update.Content.Type == "text" {
			w.thinking.WriteString(n.Update.Content.Text)
		}
	case "usage_update":
		w.usage.ContextTokens = n.Update.Used
		w.usage.ContextWindow = n.Update.Size
	}
	w.mu.Unlock()
	if !active || !target.Valid() {
		if n.Update.Type == "available_commands_update" && len(n.Update.AvailableCommands) > 0 {
			names := make([]string, 0, len(n.Update.AvailableCommands))
			for _, x := range n.Update.AvailableCommands {
				names = append(names, x.Name)
			}
			w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticDebug, Code: "commands", Detail: bounded(strings.Join(names, ", "))})
		}
		return
	}
	switch n.Update.Type {
	case "plan":
		parts := make([]string, 0, len(n.Update.Plan))
		for _, p := range n.Update.Plan {
			parts = append(parts, p.Status+": "+p.Content)
		}
		if len(parts) > 0 {
			w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NotePlan, Text: bounded(strings.Join(parts, "\n"))})
		}
	case "tool_call", "tool_call_update":
		if strings.Contains(strings.ToLower(n.Update.Title), "mcp__atoll__") {
			return
		}
		phase := driverproto.ToolStarted
		status := driverproto.ToolStatusUnknown
		if n.Update.Status == "completed" || n.Update.Status == "failed" {
			phase = driverproto.ToolEnded
			status = driverproto.ToolStatusCompleted
			if n.Update.Status == "failed" {
				status = driverproto.ToolStatusFailed
			}
		}
		name := n.Update.Title
		if name == "" {
			name = n.Update.Kind
		}
		w.publish(driverproto.Tool{Target: target, CallID: n.Update.ToolCallID, Phase: phase, Name: name, Status: status, Detail: bounded(fmt.Sprintf("%s %s", name, n.Update.Status)), Input: n.Update.RawInput, Output: n.Update.RawOutput})
	}
}
