package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type parentFrame struct {
	ParentToolUseID json.RawMessage `json:"parent_tool_use_id"`
}
type systemFrame struct {
	Capabilities      []string `json:"capabilities"`
	ClaudeCodeVersion string   `json:"claude_code_version"`
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
	} `json:"message"`
}
type resultFrame struct {
	Subtype         string   `json:"subtype"`
	IsError         bool     `json:"is_error"`
	Result          string   `json:"result"`
	Errors          []string `json:"errors"`
	TerminalReason  string   `json:"terminal_reason"`
	UserMessageUUID string   `json:"user_message_uuid"`
	NumTurns        int      `json:"num_turns"`
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
	for _, block := range frame.Message.Content {
		switch block.Type {
		case "tool_use":
			w.publish(driverproto.Tool{Target: target, CallID: block.ID, Phase: driverproto.ToolStarted, Name: block.Name, Status: driverproto.ToolStatusUnknown, Detail: boundedSummary(block.Input)})
		case "text", "thinking":
			w.publish(driverproto.Activity{Target: target})
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
	owned := frame.UserMessageUUID == turn.U || (frame.UserMessageUUID == "" && strings.HasPrefix(frame.TerminalReason, "aborted_") && turn.interrupt.inflight)
	if !owned {
		w.mu.Unlock()
		w.unsolicited("result")
		return
	}
	target := w.target
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
	w.turn = nil
	w.attempt = 0
	w.target = driverproto.WorkerTurnTarget{}
	w.phase = phaseReady
	w.mu.Unlock()
	if cancelQueued {
		_, _ = c.wire.sendControl("interrupt", map[string]any{"cancel_queued": true}, nil)
	}
	for _, outcome := range targetGone {
		w.publish(outcome)
	}
	status, final, detail := settleResult(frame)
	w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: final, ErrorDetail: detail})
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
