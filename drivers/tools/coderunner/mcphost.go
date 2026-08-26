package coderunner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// toolBinding is what one MCP tool name stands for on the channel.
type toolBinding struct {
	target actor.ActorID
	word   string
}

// toolTable is the run's MCP tool list: every word of every resolved
// requirement (word-level manifest enforcement falls out: an unlisted name is
// not callable) plus the three host tools.
type toolTable struct {
	tools    []toolSpec
	bindings map[string]toolBinding
}

var hostTools = []toolSpec{
	{Name: toolContext, Title: "run context", Description: "This run's identity and inputs: self, channel, request_id, args, and the requirement → actor id map.",
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`)},
	{Name: toolReturn, Title: "return", Description: "Finish the run with its return value. Called once, by the runtime, after the program's run() resolves.",
		InputSchema: json.RawMessage(`{"type":"object","required":["value"],"properties":{"value":{}}}`)},
	{Name: toolFail, Title: "fail", Description: "Finish the run as failed: kind is syntax | exception | invalid_output; message and stack describe it.",
		InputSchema: json.RawMessage(`{"type":"object","required":["kind","message"],"properties":{"kind":{"type":"string"},"message":{"type":"string"},"stack":{"type":"string"}}}`)},
}

// buildToolTable describes every resolved actor and lays its words out as
// MCP tools. Pure channel reads; Node is not started until this succeeds.
func (a *coderunnerActor) buildToolTable(sys actorbase.Sys, msg actorbase.Msg, actors map[string]actor.ActorID) (*toolTable, error) {
	table := &toolTable{bindings: map[string]toolBinding{}}
	names := make([]string, 0, len(actors))
	for name := range actors {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, requirement := range names {
		id := actors[requirement]
		describeMsg, err := callAndWait(sys, msg, id, introspect.QueryDescribe, struct{}{})
		if err != nil {
			return nil, fmt.Errorf("describe %s (%s): %w", requirement, id, err)
		}
		var describe introspect.Describe
		if err := json.Unmarshal(describeMsg.Payload, &describe); err != nil {
			return nil, fmt.Errorf("decode describe of %s: %w", requirement, err)
		}
		words := make([]string, 0, len(describe.Words))
		for word := range describe.Words {
			words = append(words, word)
		}
		sort.Strings(words)
		for _, word := range words {
			spec := describe.Words[word]
			name := toolName(requirement, word)
			if _, taken := table.bindings[name]; taken {
				a.logger().Warn("coderunner tool name collision", "name", name, "requirement", requirement, "word", word)
				continue
			}
			table.bindings[name] = toolBinding{target: id, word: word}
			tool := toolSpec{
				Name: name, Title: requirement + " · " + word, Description: spec.Description,
				InputSchema: mcpInputSchema(spec.InputSchema),
				Meta:        map[string]any{metaTarget: requirement, metaWord: word, "atoll/actor": string(id)},
			}
			if len(spec.OutputSchema) > 0 {
				tool.OutputSchema = spec.OutputSchema
			}
			table.tools = append(table.tools, tool)
		}
	}
	table.tools = append(table.tools, hostTools...)
	return table, nil
}

// mcpInputSchema turns a word's input schema into a tools/call arguments
// schema. Arguments must be an object; a word whose input is not declared as
// one is called with {"$input": value}.
func mcpInputSchema(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage(`{"type":"object"}`)
	}
	var probe struct {
		Type any `json:"type"`
	}
	if json.Unmarshal(raw, &probe) == nil {
		if typ, ok := probe.Type.(string); ok && typ == "object" {
			return raw
		}
	}
	wrapped, _ := json.Marshal(map[string]any{
		"type": "object", "required": []string{argInput}, "additionalProperties": false,
		"properties": map[string]any{argInput: raw},
	})
	return wrapped
}

// wordInput is the inverse: tools/call arguments → the word's input value.
func wordInput(arguments json.RawMessage) json.RawMessage {
	if len(arguments) == 0 {
		return json.RawMessage("null")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(arguments, &fields) == nil && len(fields) == 1 {
		if inner, ok := fields[argInput]; ok {
			return inner
		}
	}
	return arguments
}

// pendingSet tracks in-flight tools/call requests by their JSON-RPC id so a
// notifications/cancelled or the run's end can cancel the channel request.
type pendingSet struct {
	mu    sync.Mutex
	items map[string]actorbase.Pending
}

func newPendingSet() *pendingSet { return &pendingSet{items: map[string]actorbase.Pending{}} }

func (p *pendingSet) reserve(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.items[id]; exists {
		return false
	}
	p.items[id] = nil
	return true
}

func (p *pendingSet) attach(id string, pending actorbase.Pending) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.items[id]; !exists {
		return false
	}
	p.items[id] = pending
	return true
}

func (p *pendingSet) take(id string) actorbase.Pending {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := p.items[id]
	delete(p.items, id)
	return pending
}

func (p *pendingSet) cancel(id string) {
	p.mu.Lock()
	pending := p.items[id]
	p.mu.Unlock()
	if pending != nil {
		_ = pending.Cancel()
	}
}

func (p *pendingSet) cancelAll() {
	p.mu.Lock()
	items := p.items
	p.items = map[string]actorbase.Pending{}
	p.mu.Unlock()
	for _, pending := range items {
		if pending != nil {
			_ = pending.Cancel()
		}
	}
}

type terminalFrame struct {
	result  json.RawMessage
	failure *runtimeFailure
}

// mcpHost is the MCP server side of one run: it reads the runtime's stdout,
// answers its requests, and turns its tools/call into channel requests.
type mcpHost struct {
	actor    *coderunnerActor
	sys      actorbase.Sys
	msg      actorbase.Msg
	spec     runSpec
	actors   map[string]actor.ActorID
	table    *toolTable
	run      *processRun
	logs     *logBuffer
	pending  *pendingSet
	terminal chan<- terminalFrame
	once     sync.Once
	progress float64
}

func (h *mcpHost) finish(frame terminalFrame) {
	h.once.Do(func() { h.terminal <- frame })
}

func (h *mcpHost) respond(id json.RawMessage, result any) {
	raw, err := json.Marshal(result)
	if err != nil {
		h.respondError(id, rpcInternalError, "marshal result: "+err.Error(), nil)
		return
	}
	_ = h.run.write(rpcMessage{JSONRPC: "2.0", ID: id, Result: raw})
}

func (h *mcpHost) respondError(id json.RawMessage, code int, text string, data any) {
	e := &rpcError{Code: code, Message: text}
	if data != nil {
		e.Data, _ = json.Marshal(data)
	}
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	_ = h.run.write(rpcMessage{JSONRPC: "2.0", ID: id, Error: e})
}

func (h *mcpHost) serve(stdout io.Reader, done chan<- struct{}) {
	defer close(done)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 32*1024), 16<<20)
	semaphore := make(chan struct{}, maxConcurrency)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var m rpcMessage
		if err := json.Unmarshal(line, &m); err != nil || m.JSONRPC != "2.0" {
			// Not protocol: a stray print. Keep it as output rather than
			// failing the run over it.
			h.logs.add("stdout", string(line))
			continue
		}
		switch {
		case m.isRequest():
			switch m.Method {
			case "initialize":
				h.respond(m.ID, initializeResult{
					ProtocolVersion: mcpProtocolVersion,
					Capabilities:    map[string]any{"tools": map[string]any{}, "logging": map[string]any{}},
					ServerInfo:      map[string]any{"name": "atoll-coderunner", "version": "1"},
					Instructions:    "Tools named <requirement>__<word> are the declared Atoll actors' words; _meta carries the exact pair. Call " + toolContext + " for the run's context, " + toolReturn + " / " + toolFail + " to finish.",
				})
			case "ping":
				h.respond(m.ID, map[string]any{})
			case "tools/list":
				h.respond(m.ID, toolListResult{Tools: h.table.tools})
			case "tools/call":
				semaphore <- struct{}{}
				go func(m rpcMessage) {
					defer func() { <-semaphore }()
					h.handleToolCall(m)
				}(m)
			default:
				h.respondError(m.ID, rpcMethodNotFound, "method not found: "+m.Method, nil)
			}
		case m.isNotification():
			switch m.Method {
			case "notifications/initialized":
			case "notifications/message":
				h.onLog(m.Params)
			case "notifications/progress":
				h.onProgress(m.Params)
			case "notifications/cancelled":
				var p cancelledNotification
				if json.Unmarshal(m.Params, &p) == nil && len(p.RequestID) > 0 {
					h.pending.cancel(string(p.RequestID))
				}
			}
		case m.isResponse():
			// The host sends no requests of its own; nothing to correlate.
		}
	}
	if err := scanner.Err(); err != nil {
		h.logs.add("stdout", err.Error())
		h.logs.forceExceeded()
	}
}

func (h *mcpHost) onLog(params json.RawMessage) {
	var p loggingMessage
	if err := json.Unmarshal(params, &p); err != nil {
		return
	}
	text := ""
	var asString string
	if json.Unmarshal(p.Data, &asString) == nil {
		text = asString
	} else {
		text = string(p.Data)
	}
	stream := "stdout"
	switch {
	case p.Logger == "atoll":
		stream = "log"
	case p.Level == "warning" || p.Level == "error" || p.Level == "critical" || p.Level == "alert" || p.Level == "emergency":
		stream = "stderr"
	}
	h.logs.add(stream, text)
}

func (h *mcpHost) onProgress(params json.RawMessage) {
	var p progressNotification
	if err := json.Unmarshal(params, &p); err != nil {
		return
	}
	status := p.Message
	if !message.IsProvisionalCoreStatus(status) {
		status = message.StatusProcessing
	}
	var value any
	if len(p.Value) > 0 {
		_ = json.Unmarshal(p.Value, &value)
	}
	_, _ = h.sys.Progress(h.msg, status, value)
}

func (h *mcpHost) handleToolCall(m rpcMessage) {
	var params toolCallParams
	if err := json.Unmarshal(m.Params, &params); err != nil || params.Name == "" {
		h.respondError(m.ID, rpcInvalidParams, "tools/call needs a tool name", nil)
		return
	}
	switch params.Name {
	case toolContext:
		actors := make(map[string]string, len(h.actors))
		for requirement, id := range h.actors {
			actors[requirement] = string(id)
		}
		h.respondTool(m.ID, map[string]any{
			"self": string(h.sys.Self()), "channel": string(h.actor.deps.ChannelID), "request_id": string(h.msg.ID),
			"args": h.spec.args, "actors": actors,
		})
		return
	case toolReturn:
		var args struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(params.Arguments, &args); err != nil || len(args.Value) == 0 {
			h.respondError(m.ID, rpcInvalidParams, toolReturn+" needs value", nil)
			return
		}
		h.respondTool(m.ID, map[string]any{})
		h.finish(terminalFrame{result: append(json.RawMessage(nil), args.Value...)})
		return
	case toolFail:
		var args runtimeFailure
		if err := json.Unmarshal(params.Arguments, &args); err != nil || args.Kind == "" {
			h.respondError(m.ID, rpcInvalidParams, toolFail+" needs kind and message", nil)
			return
		}
		h.respondTool(m.ID, map[string]any{})
		h.finish(terminalFrame{failure: &runtimeFailure{Kind: args.Kind, Message: args.Message, Stack: args.Stack}})
		return
	}
	binding, ok := h.table.bindings[params.Name]
	if !ok {
		h.respondError(m.ID, rpcInvalidParams, "unknown tool: "+params.Name,
			map[string]string{"code": "undeclared_capability", "detail": params.Name + " is not a word of any declared requirement"})
		return
	}
	id := string(m.ID)
	if !h.pending.reserve(id) {
		h.respondError(m.ID, rpcInvalidRequest, "request id is in flight", nil)
		return
	}
	defer h.pending.take(id)
	input := wordInput(params.Arguments)
	var pd actorbase.Pending
	var err error
	if h.spec.forward {
		pd, err = h.sys.CallFor(h.msg.Cause(), actorbase.EffectiveCaller(h.msg), binding.target, binding.word, input)
	} else {
		pd, err = h.sys.Call(h.msg.Cause(), binding.target, binding.word, input)
	}
	if err != nil {
		h.respondToolError(m.ID, "call_failed", err.Error())
		return
	}
	if !h.pending.attach(id, pd) {
		_ = pd.Cancel()
		return
	}
	wait := defaultCallDeadline
	if raw, ok := params.Meta[metaDeadline]; ok {
		var deadlineMS int64
		if json.Unmarshal(raw, &deadlineMS) == nil && deadlineMS > 0 {
			if deadlineMS > math.MaxInt64/int64(time.Millisecond) {
				wait = time.Duration(math.MaxInt64)
			} else {
				wait = time.Duration(deadlineMS) * time.Millisecond
			}
		}
	}
	wait = boundedWait(h.msg.Ctx(), wait)
	response, err := pd.Wait(h.msg.Ctx(), wait)
	if err != nil {
		_ = pd.Cancel()
		code := "call_failed"
		if errors.Is(err, context.Canceled) {
			code = "cancelled"
		}
		h.respondToolError(m.ID, code, err.Error())
		return
	}
	var outcome struct {
		Status string `json:"status"`
		message.Failure
	}
	_ = json.Unmarshal(response.Payload, &outcome)
	if outcome.Status != message.StatusCompleted {
		code := outcome.ErrorCode
		if code == "" {
			code = "call_failed"
		}
		h.respondToolError(m.ID, code, outcome.Detail)
		return
	}
	h.respondToolRaw(m.ID, response.Payload)
}

func (h *mcpHost) respondTool(id json.RawMessage, structured any) {
	raw, err := json.Marshal(structured)
	if err != nil {
		h.respondError(id, rpcInternalError, err.Error(), nil)
		return
	}
	h.respondToolRaw(id, raw)
}

func (h *mcpHost) respondToolRaw(id json.RawMessage, structured json.RawMessage) {
	h.respond(id, toolCallResult{
		Content:           []contentBlock{{Type: "text", Text: string(structured)}},
		StructuredContent: structured,
	})
}

func (h *mcpHost) respondToolError(id json.RawMessage, code, detail string) {
	structured, _ := json.Marshal(map[string]string{"error_code": code, "detail": detail})
	h.respond(id, toolCallResult{
		Content:           []contentBlock{{Type: "text", Text: code + ": " + detail}},
		StructuredContent: structured,
		IsError:           true,
	})
}
