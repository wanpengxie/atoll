package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

type parentFrame struct {
	ParentToolUseID json.RawMessage `json:"parent_tool_use_id"`
}
type systemFrame struct {
	Capabilities      []string `json:"capabilities"`
	ClaudeCodeVersion string   `json:"claude_code_version"`
	Tools             []string `json:"tools"`
	MCPServers        []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"mcp_servers"`
}
type contentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}
type messageFrame struct {
	Message struct {
		Content []contentBlock  `json:"content"`
		Error   json.RawMessage `json:"error"`
		Model   string          `json:"model"`
	} `json:"message"`
}
type resultUsage struct {
	InputTokens         int64 `json:"input_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadTokens     int64 `json:"cache_read_input_tokens"`
}
type resultFrame struct {
	Subtype         string      `json:"subtype"`
	IsError         bool        `json:"is_error"`
	Result          string      `json:"result"`
	Errors          []string    `json:"errors"`
	TerminalReason  string      `json:"terminal_reason"`
	UserMessageUUID string      `json:"user_message_uuid"`
	NumTurns        int         `json:"num_turns"`
	Usage           resultUsage `json:"usage"`
}

type statusFrame struct {
	CompactResult string `json:"compact_result"`
	CompactError  string `json:"compact_error"`
}

type compactBoundaryFrame struct {
	Metadata struct {
		PreTokens  int64 `json:"pre_tokens"`
		PostTokens int64 `json:"post_tokens"`
		DurationMS int64 `json:"duration_ms"`
	} `json:"compact_metadata"`
}

type contextUsageFrame struct {
	TotalTokens int64 `json:"totalTokens"`
	MaxTokens   int64 `json:"maxTokens"`
}

func (w *worker) onLifecycle(c *connection, id, state string) {
	w.mu.Lock()
	if w.conn != c || w.turn == nil {
		w.mu.Unlock()
		w.debug("late_lifecycle", id+":"+state)
		return
	}
	turn := w.turn
	states := turn.seen[id]
	if states == nil {
		states = map[string]bool{}
		turn.seen[id] = states
	}
	if states[state] {
		w.mu.Unlock()
		return
	}
	states[state] = true
	if id == turn.U {
		if state == "started" && w.phase == phaseStarting {
			w.phase = phaseActive
			w.target.Native = driverproto.WorkerTurnRef(turn.U)
			target := w.target
			w.mu.Unlock()
			w.publish(driverproto.TurnStarted{Target: target})
			return
		}
		w.mu.Unlock()
		return
	}
	steer, ok := turn.steers[id]
	if !ok {
		w.mu.Unlock()
		w.debug("late_lifecycle", id+":"+state)
		return
	}
	target := w.target
	switch state {
	case "queued":
		if !steer.accepted && !steer.done {
			steer.accepted = true
			turn.steers[id] = steer
			action := steer.action
			w.mu.Unlock()
			w.publish(driverproto.ControlOutcome{Action: action, Target: target, Verdict: driverproto.ControlAccepted, Disposition: driverproto.KeepWorker})
			return
		}
	case "cancelled":
		if !steer.accepted {
			steer.done = true
			delete(turn.steers, id)
			action := steer.action
			w.mu.Unlock()
			w.publish(driverproto.ControlOutcome{Action: action, Target: target, Verdict: driverproto.ControlRejected, Detail: "cancelled before queued", Disposition: driverproto.KeepWorker})
			return
		}
		steer.done = true
		turn.steers[id] = steer
	case "started":
		if steer.accepted {
			steer.started, steer.done = true, true
			turn.steers[id] = steer
		}
	case "completed":
		if steer.accepted {
			steer.done = true
			turn.steers[id] = steer
		}
	}
	w.mu.Unlock()
}

func (w *worker) onFrame(c *connection, typ, subtype string, raw json.RawMessage) {
	var parent parentFrame
	if json.Unmarshal(raw, &parent) == nil && nonNull(parent.ParentToolUseID) {
		return
	}
	switch typ {
	case "system":
		if subtype == "init" {
			w.onInit(c, raw)
			return
		}
		if subtype == "thinking_tokens" {
			if target, ok := w.activeTarget(c); ok {
				w.publish(driverproto.Activity{Target: target})
			} else {
				w.unsolicited(typ + "/" + subtype)
			}
			return
		}
		if subtype == "status" {
			w.onStatus(c, raw)
			return
		}
		if subtype == "compact_boundary" {
			w.onCompactBoundary(c, raw)
			return
		}
		w.debug("noise_frame", typ+"/"+subtype)
	case "assistant":
		w.onAssistant(c, raw)
	case "user":
		w.onUser(c, raw)
	case "result":
		w.onResult(c, raw)
	case "rate_limit_event", "background_tasks_changed":
		w.debug("noise_frame", typ)
	default:
		w.debug("noise_frame", typ+"/"+subtype)
	}
}

func (w *worker) onStatus(c *connection, raw json.RawMessage) {
	var frame statusFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.debug("invalid_frame", "system/status")
		return
	}
	w.mu.Lock()
	if w.conn != c || w.turn == nil || w.turn.kind != driverproto.TurnCompact {
		w.mu.Unlock()
		w.debug("noise_frame", "system/status")
		return
	}
	if frame.CompactResult != "" {
		w.turn.compactResult, w.turn.compactError = frame.CompactResult, frame.CompactError
	}
	target := w.target
	w.mu.Unlock()
	if target.Valid() {
		w.publish(driverproto.Activity{Target: target})
	}
}

func (w *worker) onCompactBoundary(c *connection, raw json.RawMessage) {
	var frame compactBoundaryFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.debug("invalid_frame", "system/compact_boundary")
		return
	}
	w.mu.Lock()
	current := w.conn == c && w.turn != nil && w.turn.kind == driverproto.TurnCompact
	if current {
		w.turn.compactPost = frame.Metadata.PostTokens
		w.turn.compactMeta = true
	}
	w.mu.Unlock()
	if !current {
		w.debug("noise_frame", "system/compact_boundary")
		return
	}
	detail, _ := json.Marshal(map[string]int64{"pre_tokens": frame.Metadata.PreTokens, "post_tokens": frame.Metadata.PostTokens, "duration_ms": frame.Metadata.DurationMS})
	w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticInfo, Code: "compact", Detail: string(detail)})
}

func nonNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) != 0 && !bytes.Equal(trimmed, []byte("null"))
}

func (w *worker) activeTarget(c *connection) (driverproto.WorkerTurnTarget, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.target, w.conn == c && w.phase == phaseActive && w.turn != nil && w.target.Valid()
}

func (w *worker) onInit(c *connection, raw json.RawMessage) {
	var frame systemFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.protocolFault(c, "invalid init frame")
		return
	}
	w.mu.Lock()
	if w.conn != c || w.phase == phaseRetiring || w.phase == phaseReaped {
		w.mu.Unlock()
		return
	}
	first := !w.initSeen
	w.initSeen = true
	w.mu.Unlock()
	if !first {
		w.debug("claude_code_version", frame.ClaudeCodeVersion)
		return
	}
	set := map[string]bool{}
	for _, capability := range frame.Capabilities {
		set[capability] = true
	}
	var missing []string
	for _, capability := range []string{"interrupt_receipt_v1", "interrupt_cancel_queued_v1", "msg_lifecycle_v1"} {
		if !set[capability] {
			missing = append(missing, capability)
		}
	}
	if len(missing) != 0 {
		detail := "missing Claude Code capabilities: " + strings.Join(missing, ", ")
		w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticError, Code: "capability_missing", Detail: detail})
		w.terminal(driverproto.WorkerEnded{Cause: driverproto.WorkerProtocolFault, Detail: detail})
		return
	}
	if w.host != nil && w.host.Logger() != nil {
		w.host.Logger().Debug("claude.system_init", "mcp_servers", frame.MCPServers, "tools", frame.Tools)
	}
	w.debug("claude_code_version", frame.ClaudeCodeVersion)
}

func (w *worker) protocolFault(c *connection, detail string) {
	w.mu.Lock()
	current := w.conn == c && w.phase != phaseRetiring && w.phase != phaseReaped
	w.mu.Unlock()
	if current {
		w.terminal(driverproto.WorkerEnded{Cause: driverproto.WorkerProtocolFault, Detail: detail})
	}
}

func (w *worker) onAssistant(c *connection, raw json.RawMessage) {
	target, ok := w.activeTarget(c)
	if !ok {
		w.unsolicited("assistant")
		return
	}
	var frame messageFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.debug("invalid_frame", "assistant")
		return
	}
	if frame.Message.Model != "" && frame.Message.Model != "<synthetic>" {
		w.mu.Lock()
		w.lastModel = frame.Message.Model
		w.mu.Unlock()
	}
	for _, block := range frame.Message.Content {
		switch block.Type {
		case "tool_use":
			if strings.HasPrefix(block.Name, toolsurface.ClaudeExposedPrefix) {
				// Host-served tool: the host callback projects this call
				// authoritatively; the stream narration is liveness only.
				w.mu.Lock()
				w.hostToolCalls[block.ID] = struct{}{}
				w.mu.Unlock()
				w.publish(driverproto.Activity{Target: target})
				continue
			}
			w.publish(driverproto.Tool{Target: target, CallID: block.ID, Phase: driverproto.ToolStarted, Name: block.Name, Status: driverproto.ToolStatusUnknown, Detail: boundedSummary(block.Input)})
		case "text":
			// 到达的 text/thinking 块是完成了的中间产物（stream-json 按块整发，
			// 不是 delta）——按统一词表填充成 ProgressNote；空块只算活性。
			if strings.TrimSpace(block.Text) != "" {
				w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NoteText, Text: boundedSummary(strings.TrimSpace(block.Text))})
			} else {
				w.publish(driverproto.Activity{Target: target})
			}
		case "thinking":
			if strings.TrimSpace(block.Thinking) != "" {
				w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NoteThinking, Text: boundedSummary(strings.TrimSpace(block.Thinking))})
			} else {
				w.publish(driverproto.Activity{Target: target})
			}
		}
	}
	if nonNull(frame.Message.Error) {
		w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticWarn, Code: "assistant_error", Detail: boundedSummary(frame.Message.Error)})
	}
}

func (w *worker) onUser(c *connection, raw json.RawMessage) {
	target, ok := w.activeTarget(c)
	if !ok {
		w.unsolicited("user")
		return
	}
	var frame messageFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.debug("invalid_frame", "user")
		return
	}
	hadTool := false
	for _, block := range frame.Message.Content {
		if block.Type != "tool_result" {
			continue
		}
		hadTool = true
		w.mu.Lock()
		_, hostServed := w.hostToolCalls[block.ToolUseID]
		if hostServed {
			delete(w.hostToolCalls, block.ToolUseID)
		}
		w.mu.Unlock()
		if hostServed {
			continue
		}
		status := driverproto.ToolStatusCompleted
		if block.IsError {
			status = driverproto.ToolStatusFailed
		}
		w.publish(driverproto.Tool{Target: target, CallID: block.ToolUseID, Phase: driverproto.ToolEnded, Status: status, Detail: boundedSummary(block.Content)})
	}
	if !hadTool {
		w.publish(driverproto.Activity{Target: target})
	}
}

func (w *worker) onResult(c *connection, raw json.RawMessage) {
	var frame resultFrame
	if json.Unmarshal(raw, &frame) != nil {
		w.debug("invalid_frame", "result")
		return
	}
	w.mu.Lock()
	if w.conn != c || w.turn == nil {
		w.mu.Unlock()
		w.unsolicited("result")
		return
	}
	if w.phase == phaseStarting {
		attempt, resume, u := w.attempt, w.resume, w.turn.U
		w.mu.Unlock()
		if resume && frame.NumTurns == 0 && frame.IsError && invalidResumePattern.MatchString(strings.Join(frame.Errors, "; ")) {
			w.terminal(driverproto.SubmissionRejected{Attempt: attempt, Class: driverproto.FailureResumeInvalid, Detail: resultDetail(frame), Disposition: driverproto.RetireWorker})
			return
		}
		if frame.UserMessageUUID == u {
			w.terminal(driverproto.SubmissionRejected{Attempt: attempt, Class: driverproto.FailureProvider, Detail: resultDetail(frame), Disposition: driverproto.RetireWorker})
			return
		}
		w.unsolicited("result")
		return
	}
	if w.phase != phaseActive || !w.target.Valid() {
		w.mu.Unlock()
		w.unsolicited("result")
		return
	}
	turn := w.turn
	owned := frame.UserMessageUUID == turn.U || (frame.UserMessageUUID == "" && strings.HasPrefix(frame.TerminalReason, "aborted_") && turn.interrupt.inflight) || (frame.UserMessageUUID == "" && frame.NumTurns == 0 && turn.kind != driverproto.TurnChat)
	if !owned {
		w.mu.Unlock()
		w.unsolicited("result")
		return
	}
	target := w.target
	if turn.settling {
		w.mu.Unlock()
		return
	}
	turn.settling = true
	var cancelQueued bool
	var targetGone []driverproto.ControlOutcome
	for _, steer := range turn.steers {
		if steer.started || steer.done {
			continue
		}
		cancelQueued = true
		if !steer.accepted {
			targetGone = append(targetGone, driverproto.ControlOutcome{Action: steer.action, Target: target, Verdict: driverproto.ControlTargetGone, Detail: "turn ended before steer queued", Disposition: driverproto.KeepWorker})
		}
	}
	w.mu.Unlock()
	if cancelQueued {
		_, _ = c.wire.sendControl("interrupt", map[string]any{"cancel_queued": true}, nil)
	}
	for _, outcome := range targetGone {
		w.publish(outcome)
	}
	w.finishResult(c, target, frame)
}

func (w *worker) finishResult(c *connection, target driverproto.WorkerTurnTarget, frame resultFrame) {
	w.mu.Lock()
	if w.conn != c || w.turn == nil || !w.turn.settling || w.target != target {
		w.mu.Unlock()
		return
	}
	turn := w.turn
	usage := w.currentUsageLocked()
	switch turn.kind {
	case driverproto.TurnChat:
		usage.ContextTokens = frame.Usage.InputTokens + frame.Usage.CacheCreationTokens + frame.Usage.CacheReadTokens
	case driverproto.TurnCompact:
		if turn.compactMeta {
			usage.ContextTokens = turn.compactPost
		}
	case driverproto.TurnSelect:
		// Claude reports zero result usage for control turns. Preserve the
		// latest context usage while applying the selected model and effort.
	}
	if turn.kind == driverproto.TurnSelect {
		if frame.Subtype == "success" {
			w.options = turn.options
		}
		usage.Model, usage.Effort = w.options.Model, w.options.Effort
	}
	w.usage = usage
	w.turn = nil
	w.attempt = 0
	w.target = driverproto.WorkerTurnTarget{}
	w.phase = phaseReady
	w.mu.Unlock()
	status, final, detail := settleResult(frame)
	if turn.kind == driverproto.TurnCompact {
		final = ""
		if turn.compactResult == "failed" {
			status, detail = driverproto.TurnFailed, turn.compactError
		}
	}
	if turn.kind == driverproto.TurnSelect {
		final = ""
	}
	w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: final, ErrorDetail: detail, Usage: usage})
}

func resultDetail(frame resultFrame) string {
	if len(frame.Errors) != 0 {
		return strings.Join(frame.Errors, "; ")
	}
	if frame.Subtype != "" {
		return frame.Subtype
	}
	return "Claude Code result before turn started"
}

func settleResult(frame resultFrame) (driverproto.TurnEndStatus, string, string) {
	if frame.Subtype == "success" {
		return driverproto.TurnOK, frame.Result, ""
	}
	if strings.HasPrefix(frame.TerminalReason, "aborted_") {
		return driverproto.TurnInterrupted, "", ""
	}
	if frame.IsError {
		return driverproto.TurnFailed, "", resultDetail(frame)
	}
	return driverproto.TurnFailed, "", "unexpected result subtype " + frame.Subtype
}

func (w *worker) unsolicited(kind string) {
	w.mu.Lock()
	level := driverproto.DiagnosticDebug
	if !w.unsolicitedWarned {
		w.unsolicitedWarned = true
		level = driverproto.DiagnosticWarn
	}
	w.mu.Unlock()
	w.publish(driverproto.Diagnostic{Level: level, Code: "unsolicited_cycle", Detail: kind})
}

func (w *worker) debug(code, detail string) {
	w.mu.Lock()
	if w.phase == phaseRetiring || w.phase == phaseReaped || w.debugSeen[code+"\x00"+detail] {
		w.mu.Unlock()
		return
	}
	w.debugSeen[code+"\x00"+detail] = true
	w.mu.Unlock()
	w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticDebug, Code: code, Detail: detail})
}

func boundedSummary(v any) string {
	var s string
	switch value := v.(type) {
	case string:
		s = value
	case json.RawMessage:
		s = compactJSON(value)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			s = fmt.Sprint(value)
		} else {
			s = compactJSON(raw)
		}
	}
	s = redactNative(s)
	runes := []rune(s)
	if len(runes) <= summaryMaxChars {
		return s
	}
	mark := "…[truncated]"
	return string(runes[:summaryMaxChars-utf8.RuneCountInString(mark)]) + mark
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
