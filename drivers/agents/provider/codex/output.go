package codex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/registry"
)

const watchdogInitialTimeout = 10 * time.Minute

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

func (e *engine) handleNotification(c *connection, method string, params json.RawMessage) {
	if !e.isCurrent(c) {
		return
	}
	switch method {
	case "turn/started":
		var n turnNotice
		if json.Unmarshal(params, &n) != nil || !e.threadMatches(n.ThreadID) {
			return
		}
		e.mu.Lock()
		op := e.startOp
		if op == "" || n.Turn.ID == "" {
			e.mu.Unlock()
			return
		}
		e.startOp = ""
		e.turnID = n.Turn.ID
		e.final[n.Turn.ID] = ""
		e.mu.Unlock()
		e.events.TurnStarted(op, n.Turn.ID)
		e.feedWatchdog(n.Turn.ID)
	case "turn/completed":
		var n turnNotice
		if json.Unmarshal(params, &n) != nil || !e.activeNotice(n.ThreadID, n.Turn.ID) {
			return
		}
		if n.Turn.Status == "inProgress" {
			e.feedWatchdog(n.Turn.ID)
			e.cfg.Logger.Warn("codex.turn_completed_in_progress", "turn", n.Turn.ID)
			return
		}
		e.stopWatchdog()
		e.mu.Lock()
		final := e.final[n.Turn.ID]
		delete(e.final, n.Turn.ID)
		e.turnID = ""
		e.mu.Unlock()
		status := base.TurnStatusOK
		detail := ""
		switch n.Turn.Status {
		case "completed":
		case "interrupted":
			status = base.TurnStatusInterrupted
		case "failed":
			status = base.TurnStatusFailed
			if n.Turn.Error != nil {
				detail = n.Turn.Error.Message
				if n.Turn.Error.Additional != "" {
					detail += "; " + n.Turn.Error.Additional
				}
			}
		default:
			status = base.TurnStatusFailed
			detail = "unknown codex turn status: " + n.Turn.Status
		}
		e.events.TurnEnded(n.Turn.ID, status, final, detail)
	case "item/started", "item/completed":
		var n itemNotice
		if json.Unmarshal(params, &n) != nil || !e.activeNotice(n.ThreadID, n.TurnID) {
			return
		}
		e.feedWatchdog(n.TurnID)
		e.handleItem(method, n)
	case "error":
		var n struct {
			ThreadID  string `json:"threadId"`
			TurnID    string `json:"turnId"`
			WillRetry bool   `json:"willRetry"`
			Error     struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(params, &n) == nil && e.activeNotice(n.ThreadID, n.TurnID) {
			e.feedWatchdog(n.TurnID)
			e.cfg.Logger.Warn("codex.turn_error", "turn", n.TurnID, "will_retry", n.WillRetry, "detail", n.Error.Message)
		}
	case "deprecationNotice":
		e.cfg.Logger.Warn("codex.deprecation_notice", "payload", string(params))
	default:
		if isDeltaMethod(method) {
			return
		}
	}
}

func (e *engine) threadMatches(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return id != "" && id == e.threadID
}
func (e *engine) activeNotice(thread, turn string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return thread == e.threadID && turn != "" && turn == e.turnID
}
func (e *engine) handleItem(method string, n itemNotice) {
	phase := "started"
	if method == "item/completed" {
		phase = "ended"
	}
	if n.Item.Type == "agentMessage" {
		if phase == "ended" && strings.TrimSpace(n.Item.Text) != "" {
			e.mu.Lock()
			e.final[n.TurnID] = n.Item.Text
			e.mu.Unlock()
		}
		return
	}
	if n.Item.Type == "userMessage" || n.Item.Type == "reasoning" || n.Item.Type == "plan" || n.Item.Type == "contextCompaction" {
		return
	}
	name := n.Item.Tool
	if name == "" {
		name = n.Item.Type
	}
	status := ""
	if phase == "ended" {
		status = registry.ActivityToolEndedStatusCompleted
		if strings.EqualFold(n.Item.Status, "failed") {
			status = registry.ActivityToolEndedStatusFailed
		}
	}
	e.events.Tool(n.TurnID, n.Item.ID, phase, name, status, boundedToolSummary(n.Item))
}
func boundedToolSummary(item itemWire) string {
	s := toolSummary(item)
	r := []rune(s)
	if len(r) <= toolSummaryMaxChars {
		return s
	}
	mark := "…[truncated]"
	limit := toolSummaryMaxChars - utf8.RuneCountInString(mark)
	return string(r[:limit]) + mark
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
		targets := strings.Join(item.ReceiverThreadIDs, ",")
		return joinToolSummary(item.Tool, targets, item.Prompt, item.Status)
	default:
		return firstNonEmpty(item.Command, item.AggregatedOutput, item.Status)
	}
}

func joinToolSummary(parts ...string) string {
	nonEmpty := parts[:0]
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	return strings.Join(nonEmpty, " · ")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
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

func (e *engine) feedWatchdog(turn string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if turn == "" || turn != e.turnID {
		return
	}
	if e.watchdog != nil {
		e.watchdog.Stop()
	}
	c := e.current
	e.watchdog = time.AfterFunc(watchdogInitialTimeout, func() {
		e.mu.Lock()
		if e.turnID != turn || e.current != c {
			e.mu.Unlock()
			return
		}
		thread := e.threadID
		e.current = nil
		e.turnID = ""
		e.mu.Unlock()
		if c != nil {
			_, _ = c.rpc.call(e.life, "turn/interrupt", map[string]any{"threadId": thread, "turnId": turn}, time.Second)
			c.retire()
		}
		e.events.ProviderLost(base.LostTimeout, fmt.Sprintf("turn %s made no progress", turn))
	})
}
func (e *engine) stopWatchdog() {
	e.mu.Lock()
	if e.watchdog != nil {
		e.watchdog.Stop()
		e.watchdog = nil
	}
	e.mu.Unlock()
}
