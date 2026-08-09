package codex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type turnWire struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  *struct {
		Message    string `json:"message"`
		Additional string `json:"additionalDetails"`
	} `json:"error"`
}
type turnNotice struct {
	ThreadID string   `json:"threadId"`
	Turn     turnWire `json:"turn"`
}
type itemWire struct {
	ID                string            `json:"id"`
	Type              string            `json:"type"`
	Text              string            `json:"text"`
	Tool              string            `json:"tool"`
	Server            string            `json:"server"`
	Namespace         string            `json:"namespace"`
	Command           string            `json:"command"`
	Status            string            `json:"status"`
	AggregatedOutput  string            `json:"aggregatedOutput"`
	Arguments         json.RawMessage   `json:"arguments"`
	Changes           []json.RawMessage `json:"changes"`
	Query             string            `json:"query"`
	Path              string            `json:"path"`
	SavedPath         string            `json:"savedPath"`
	Result            json.RawMessage   `json:"result"`
	Prompt            string            `json:"prompt"`
	SenderThreadID    string            `json:"senderThreadId"`
	ReceiverThreadIDs []string          `json:"receiverThreadIds"`
}
type itemNotice struct {
	ThreadID string   `json:"threadId"`
	TurnID   string   `json:"turnId"`
	Item     itemWire `json:"item"`
}

func (w *worker) notification(c *connection, method string, params json.RawMessage) {
	w.mu.Lock()
	if w.conn != c && w.conn != nil {
		w.mu.Unlock()
		return
	}
	thread, attempt, target := w.thread, w.attempt, w.target
	w.mu.Unlock()
	switch method {
	case "turn/started":
		var n turnNotice
		if json.Unmarshal(params, &n) != nil || n.Turn.ID == "" || n.ThreadID != thread || attempt == 0 {
			return
		}
		target = driverproto.WorkerTurnTarget{Attempt: attempt, Native: driverproto.WorkerTurnRef(n.Turn.ID)}
		w.mu.Lock()
		if w.attempt != attempt || w.phase != phaseStarting {
			w.mu.Unlock()
			return
		}
		w.target = target
		w.phase = phaseActive
		w.final[target.Native] = ""
		w.mu.Unlock()
		w.publish(driverproto.TurnStarted{Target: target})
	case "turn/completed":
		var n turnNotice
		if json.Unmarshal(params, &n) != nil || target.Native != driverproto.WorkerTurnRef(n.Turn.ID) || n.ThreadID != thread {
			return
		}
		w.mu.Lock()
		if w.phase != phaseActive || w.target != target {
			w.mu.Unlock()
			return
		}
		final := w.final[target.Native]
		delete(w.final, target.Native)
		w.attempt = 0
		w.target = driverproto.WorkerTurnTarget{}
		w.phase = phaseReady
		w.mu.Unlock()
		status := driverproto.TurnOK
		detail := ""
		switch n.Turn.Status {
		case "completed":
		case "interrupted":
			status = driverproto.TurnInterrupted
		case "failed":
			status = driverproto.TurnFailed
			if n.Turn.Error != nil {
				detail = n.Turn.Error.Message
				if n.Turn.Error.Additional != "" {
					detail += "; " + n.Turn.Error.Additional
				}
			}
		default:
			status = driverproto.TurnFailed
			detail = "unknown codex turn status: " + n.Turn.Status
		}
		w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: final, ErrorDetail: detail})
	case "item/started", "item/completed":
		var n itemNotice
		if json.Unmarshal(params, &n) != nil || n.ThreadID != thread || driverproto.WorkerTurnRef(n.TurnID) != target.Native {
			return
		}
		if n.Item.Type == "agentMessage" {
			if method == "item/completed" && strings.TrimSpace(n.Item.Text) != "" {
				w.mu.Lock()
				w.final[target.Native] = n.Item.Text
				w.mu.Unlock()
			}
			return
		}
		if n.Item.Type == "userMessage" || n.Item.Type == "reasoning" || n.Item.Type == "plan" || n.Item.Type == "contextCompaction" {
			w.publish(driverproto.Activity{Target: target})
			return
		}
		phase := driverproto.ToolStarted
		if method == "item/completed" {
			phase = driverproto.ToolEnded
		}
		name := n.Item.Tool
		if name == "" {
			name = n.Item.Type
		}
		status := driverproto.ToolStatusUnknown
		if phase == driverproto.ToolEnded {
			status = driverproto.ToolStatusCompleted
			if strings.EqualFold(n.Item.Status, "failed") {
				status = driverproto.ToolStatusFailed
			}
		}
		w.publish(driverproto.Tool{Target: target, CallID: n.Item.ID, Phase: phase, Name: name, Status: status, Detail: boundedToolSummary(n.Item)})
	case "error":
		var notice struct {
			ThreadID  string `json:"threadId"`
			TurnID    string `json:"turnId"`
			WillRetry bool   `json:"willRetry"`
			Error     struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(params, &notice) == nil && notice.ThreadID == thread && driverproto.WorkerTurnRef(notice.TurnID) == target.Native && target.Valid() {
			w.publish(driverproto.Activity{Target: target})
		}
	default:
		if isDeltaMethod(method) && target.Valid() {
			w.publish(driverproto.Activity{Target: target})
		}
	}
}

func boundedToolSummary(item itemWire) string {
	s := redactNative(toolSummary(item))
	r := []rune(s)
	if len(r) <= toolSummaryMaxChars {
		return s
	}
	mark := "…[truncated]"
	return string(r[:toolSummaryMaxChars-utf8.RuneCountInString(mark)]) + mark
}
func toolSummary(item itemWire) string {
	switch item.Type {
	case "commandExecution":
		return firstNonEmpty(item.Command, item.AggregatedOutput, item.Status)
	case "fileChange":
		return fmt.Sprintf("%d file change(s)", len(item.Changes))
	case "mcpToolCall":
		return joinToolSummary(item.Server, item.Tool, compactJSON(item.Arguments), item.Status)
	case "dynamicToolCall":
		return joinToolSummary(item.Namespace, item.Tool, compactJSON(item.Arguments), item.Status)
	case "webSearch":
		return firstNonEmpty(item.Query, item.Status)
	case "imageView":
		return item.Path
	case "imageGeneration":
		return firstNonEmpty(item.SavedPath, compactJSON(item.Result), item.Status)
	case "collabAgentToolCall":
		return joinToolSummary(item.Tool, strings.Join(item.ReceiverThreadIDs, ","), item.Prompt, item.Status)
	default:
		return firstNonEmpty(item.Command, item.AggregatedOutput, item.Status)
	}
}
func joinToolSummary(parts ...string) string {
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, " · ")
}
func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var out bytes.Buffer
	if json.Compact(&out, raw) == nil {
		return out.String()
	}
	return string(raw)
}
func isDeltaMethod(method string) bool { return strings.Contains(strings.ToLower(method), "delta") }
