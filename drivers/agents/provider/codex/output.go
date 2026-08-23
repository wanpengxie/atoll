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
	Phase             string            `json:"phase"`
	SenderThreadID    string            `json:"senderThreadId"`
	ReceiverThreadIDs []string          `json:"receiverThreadIds"`
}
type itemNotice struct {
	ThreadID string   `json:"threadId"`
	TurnID   string   `json:"turnId"`
	Item     itemWire `json:"item"`
}
type tokenUsageNotice struct {
	ThreadID   string `json:"threadId"`
	TurnID     string `json:"turnId"`
	TokenUsage struct {
		Last struct {
			TotalTokens int64 `json:"totalTokens"`
		} `json:"last"`
		ModelContextWindow *int64 `json:"modelContextWindow"`
	} `json:"tokenUsage"`
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
	case "thread/tokenUsage/updated":
		var n tokenUsageNotice
		if json.Unmarshal(params, &n) != nil || n.ThreadID != thread {
			return
		}
		w.mu.Lock()
		w.usage.ContextTokens = n.TokenUsage.Last.TotalTokens
		if n.TokenUsage.ModelContextWindow != nil {
			w.usage.ContextWindow = *n.TokenUsage.ModelContextWindow
		}
		w.mu.Unlock()
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
		usage := w.currentUsageLocked()
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
		w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: final, ErrorDetail: detail, Usage: usage})
	case "item/started", "item/completed":
		var n itemNotice
		if json.Unmarshal(params, &n) != nil || n.ThreadID != thread || driverproto.WorkerTurnRef(n.TurnID) != target.Native {
			return
		}
		if n.Item.Type == "agentMessage" {
			// agentMessage 分两相：commentary 是干活途中的旁白（明文中间产物），
			// final_answer 才是终稿。不分相会把旁白写进终稿——回合若在旁白之后
			// 中断，用户收到的"回答"就是一句旁白。
			if n.Item.Phase == "commentary" {
				if method == "item/completed" && strings.TrimSpace(n.Item.Text) != "" {
					w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NoteText, Text: boundedNoteText(n.Item.Text)})
					return
				}
				w.publish(driverproto.Activity{Target: target})
				return
			}
			if method == "item/completed" && strings.TrimSpace(n.Item.Text) != "" {
				w.mu.Lock()
				w.final[target.Native] = n.Item.Text
				w.mu.Unlock()
			}
			w.publish(driverproto.Activity{Target: target})
			return
		}
		if n.Item.Type == "reasoning" {
			// 实测：reasoning 在 wire 上恒是 summary:[] content:[]（xhigh 也
			// 一样，不退订 delta 也一样）——OpenAI 不下发思考明文。但
			// started→completed 是真实的思考区间，发一条无文本的 thinking
			// note，让前端有据可依地显示"思考中"，而不是自己编。
			if method == "item/started" {
				w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NoteThinking})
				return
			}
			w.publish(driverproto.Activity{Target: target})
			return
		}
		if n.Item.Type == "plan" {
			if method == "item/completed" && strings.TrimSpace(n.Item.Text) != "" {
				w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NotePlan, Text: boundedNoteText(n.Item.Text)})
				return
			}
			w.publish(driverproto.Activity{Target: target})
			return
		}
		if n.Item.Type == "userMessage" || n.Item.Type == "contextCompaction" {
			w.publish(driverproto.Activity{Target: target})
			return
		}
		if n.Item.Type == "dynamicToolCall" {
			// Dynamic tools are host-served (item/tool/call → host callback), and
			// the host projects that call authoritatively as tool started/ended.
			// Re-publishing codex's own narration of the same call would double
			// every tool event on the ledger — count it as liveness only.
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
		w.publish(driverproto.Tool{Target: target, CallID: n.Item.ID, Phase: phase, Name: name, Status: status, Detail: boundedToolSummary(n.Item), Input: toolInput(n.Item, phase), Output: toolOutput(n.Item, phase)})
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

// boundedNoteText 是 ProgressNote 的统一整形：脱敏 + 截断，与工具摘要同一
// 长度预算。note 是摘要不是正文，超长恒截。
func boundedNoteText(s string) string {
	s = redactNative(strings.TrimSpace(s))
	r := []rune(s)
	if len(r) <= toolSummaryMaxChars {
		return s
	}
	mark := "…[truncated]"
	return string(r[:toolSummaryMaxChars-utf8.RuneCountInString(mark)]) + mark
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

func toolInput(item itemWire, phase driverproto.ToolPhase) json.RawMessage {
	if phase != driverproto.ToolStarted {
		return nil
	}
	if len(item.Arguments) != 0 && !bytes.Equal(bytes.TrimSpace(item.Arguments), []byte("null")) {
		return append(json.RawMessage(nil), item.Arguments...)
	}
	var value any
	switch item.Type {
	case "commandExecution":
		value = map[string]any{"command": item.Command}
	case "webSearch":
		value = map[string]any{"query": item.Query}
	case "imageView":
		value = map[string]any{"path": item.Path}
	case "collabAgentToolCall":
		value = map[string]any{"prompt": item.Prompt, "receiver_thread_ids": item.ReceiverThreadIDs}
	default:
		return nil
	}
	raw, _ := json.Marshal(value)
	return raw
}

func toolOutput(item itemWire, phase driverproto.ToolPhase) json.RawMessage {
	if phase != driverproto.ToolEnded {
		return nil
	}
	if len(item.Result) != 0 && !bytes.Equal(bytes.TrimSpace(item.Result), []byte("null")) {
		return append(json.RawMessage(nil), item.Result...)
	}
	if item.AggregatedOutput != "" {
		raw, _ := json.Marshal(item.AggregatedOutput)
		return raw
	}
	if len(item.Changes) != 0 {
		raw, _ := json.Marshal(item.Changes)
		return raw
	}
	return nil
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
