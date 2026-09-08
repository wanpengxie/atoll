package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

const Class = "agent-looper"
const maxResultTextBytes = 64 << 10

type Config struct {
	ControllerActor string `json:"controller_actor,omitempty"`
	MaxAssignments  int    `json:"max_assignments,omitempty"`
}

func defaultConfig() json.RawMessage {
	return json.RawMessage(`{"controller_actor":"native-agent","max_assignments":32}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{ControllerActor: "native-agent", MaxAssignments: 32}
	if len(raw) > 0 {
		if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
			return Config{}, err
		}
	}
	if cfg.MaxAssignments < 1 || cfg.MaxAssignments > 10000 {
		return Config{}, errors.New("agent-looper config: max_assignments must be 1..10000")
	}
	if strings.TrimSpace(cfg.ControllerActor) == "" {
		return Config{}, errors.New("agent-looper config: controller_actor is required")
	}
	return cfg, nil
}
func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: defaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"controller_actor":{"type":"string","minLength":1},"max_assignments":{"type":"integer","minimum":1,"maximum":10000}}}`)})
}
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg), nil }}}}, nil
}
func manifest() introspect.Manifest {
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "agent-loop"}, Capabilities: map[string]bool{"multi_assignment": true, "independent_assignment": true, "targeted_stop": true}, Words: map[string]introspect.WordSpec{
		agentloop.TypeStart:   {Description: "Accept one of a bounded set of independently-lived Agent episode assignments from the configured Controller and return before its LLM/tool loop completes.", InputSchema: json.RawMessage(agentloop.StartInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "capacity", "assignment_conflict"}},
		agentloop.TypeInput:   {Description: "Deliver one already accepted input from the configured Controller to the matching assignment; it is adopted only at a later safe context boundary.", InputSchema: json.RawMessage(agentloop.InputInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found", "operation_mismatch"}},
		agentloop.TypeStop:    {Description: "Stop only the matching assignment; a stale assignment id cannot cancel its successor.", InputSchema: json.RawMessage(agentloop.StopInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found", "operation_mismatch"}},
		agentloop.TypeInspect: {Description: "Inspect process-local assignment activity. Controller work state remains the external authority.", InputSchema: json.RawMessage(agentloop.InspectInputSchema), OutputSchema: json.RawMessage(agentloop.InspectOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found"}},
	}}
}

type assignment struct {
	start   agentloop.StartRequest
	cause   message.Cause
	cancel  context.CancelFunc
	mu      sync.Mutex
	inputs  []agentloop.Input
	phase   string
	history []json.RawMessage
}
type looper struct {
	cfg    Config
	mu     sync.Mutex
	active map[string]*assignment
	wg     sync.WaitGroup
}

func proc(cfg Config) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		l := &looper{cfg: cfg, active: map[string]*assignment{}}
		defer l.wg.Wait()
		for {
			msg, err := sys.Recv()
			if err != nil {
				l.mu.Lock()
				for _, a := range l.active {
					a.cancel()
				}
				l.mu.Unlock()
				return err
			}
			if msg.Kind != message.KindRequest {
				continue
			}
			switch msg.Type {
			case agentloop.TypeStart:
				l.start(sys, msg)
			case agentloop.TypeInput:
				l.input(sys, msg)
			case agentloop.TypeStop:
				l.stop(sys, msg)
			case agentloop.TypeInspect:
				l.inspect(sys, msg)
			default:
				_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("Agent looper does not answer %q", msg.Type))
			}
		}
	}
}

func (l *looper) start(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may start an assignment")
		return
	}
	var req agentloop.StartRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if req.WorkID == "" || req.AssignmentID == "" || req.ControllerActor == "" || req.ContextActor == "" || req.LLMActor == "" || len(req.Inputs) == 0 {
		_, _ = sys.Fail(msg, "invalid_args", "work_id, assignment_id, controller_actor, context_actor, llm_actor, and inputs are required")
		return
	}
	if req.ControllerActor != msg.Sender.ID.String() {
		_, _ = sys.Fail(msg, "invalid_args", "controller_actor must be the actual request sender")
		l.reportRejected(sys, msg, req, "controller_mismatch", "controller_actor must be the actual request sender")
		return
	}
	l.mu.Lock()
	if old := l.active[string(req.WorkID)]; old != nil {
		if old.start.AssignmentID == req.AssignmentID {
			l.mu.Unlock()
			_, _ = sys.Reply(msg, map[string]any{"disposition": "already_accepted", "assignment_id": req.AssignmentID})
			l.report(sys, old, "accepted", 0, nil, "", "", "confirmed_running")
			return
		}
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "assignment_conflict", "the work already has a different active assignment")
		l.reportRejected(sys, msg, req, "assignment_conflict", "the work already has a different active assignment")
		return
	}
	if len(l.active) >= l.cfg.MaxAssignments {
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "capacity", "looper has reached max_assignments")
		l.reportRejected(sys, msg, req, "capacity", "looper has reached max_assignments")
		return
	}
	ctx, cancel := context.WithCancel(sys.Life())
	a := &assignment{start: req, cause: msg.Cause(), cancel: cancel, inputs: append([]agentloop.Input(nil), req.Inputs...), phase: "accepted"}
	l.active[string(req.WorkID)] = a
	l.wg.Add(1)
	l.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted", "work_id": req.WorkID, "assignment_id": req.AssignmentID})
	l.report(sys, a, "accepted", 0, nil, "", "", "confirmed_running")
	go func() {
		defer l.wg.Done()
		l.drive(ctx, sys, a)
		l.mu.Lock()
		if l.active[string(req.WorkID)] == a {
			delete(l.active, string(req.WorkID))
		}
		l.mu.Unlock()
	}()
}

func (l *looper) input(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may deliver input")
		return
	}
	var req agentloop.InputRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	l.mu.Lock()
	a := l.active[string(req.WorkID)]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	if a.start.AssignmentID != req.AssignmentID {
		_, _ = sys.Fail(msg, "operation_mismatch", "input targets a stale assignment")
		return
	}
	a.mu.Lock()
	for _, in := range a.inputs {
		if in.ID == req.Input.ID {
			a.mu.Unlock()
			_, _ = sys.Reply(msg, map[string]any{"disposition": "already_accepted", "input_id": req.Input.ID})
			return
		}
	}
	a.inputs = append(a.inputs, req.Input)
	a.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted", "input_id": req.Input.ID, "included": false})
}

func (l *looper) stop(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may stop an assignment")
		return
	}
	var req agentloop.StopRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	l.mu.Lock()
	a := l.active[string(req.WorkID)]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "already_stopped", "work_id": req.WorkID})
		return
	}
	if a.start.AssignmentID != req.AssignmentID {
		_, _ = sys.Fail(msg, "operation_mismatch", "stop targets a stale assignment")
		return
	}
	a.cancel()
	_, _ = sys.Reply(msg, map[string]any{"disposition": "stop_requested", "work_id": req.WorkID, "assignment_id": req.AssignmentID})
}
func (l *looper) inspect(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may inspect an assignment")
		return
	}
	var req agentloop.InspectRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	l.mu.Lock()
	a := l.active[string(req.WorkID)]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	a.mu.Lock()
	phase, n := a.phase, len(a.inputs)
	a.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"work_id": req.WorkID, "assignment_id": a.start.AssignmentID, "phase": phase, "input_count": n})
}

func (l *looper) authorized(msg actorbase.Msg) bool {
	return targetMatches(l.cfg.ControllerActor, msg.Sender.ID.String())
}

func targetMatches(configured, actual string) bool {
	if configured == actual {
		return true
	}
	parts := strings.Split(actual, ":")
	if len(parts) != 3 {
		return false
	}
	if !strings.Contains(configured, ":") {
		return configured == parts[1]
	}
	want := strings.Split(configured, ":")
	return len(want) == 2 && want[0] == parts[0] && want[1] == parts[1]
}

func (l *looper) drive(ctx context.Context, sys actorbase.Sys, a *assignment) {
	a.mu.Lock()
	inputs := append([]agentloop.Input(nil), a.inputs...)
	a.phase = "context"
	a.mu.Unlock()
	var through int64
	for _, in := range inputs {
		if in.Seq > through {
			through = in.Seq
		}
	}
	contextRaw, err := call(ctx, sys, a.cause, actor.ActorID(a.start.ContextActor), contextproto.TypeBuild, contextproto.BuildRequest{WorkID: a.start.WorkID, AssignmentID: a.start.AssignmentID, Inputs: inputs, Prior: a.start.Prior})
	if err != nil {
		l.report(sys, a, "failed", through, nil, "context_failed", err.Error(), stateForError(err))
		return
	}
	var artifact contextproto.Artifact
	if err := json.Unmarshal(contextRaw, &artifact); err != nil {
		l.report(sys, a, "failed", through, nil, "context_invalid", err.Error(), "confirmed")
		return
	}
	history := append([]json.RawMessage(nil), artifact.Messages...)
	a.setHistory(history)
	turns := a.start.MaxTurns
	if turns <= 0 {
		turns = 12
	}
	tools := toolDefinitions(a.start.WorkspaceActor != "")
	for turn := 0; turn < turns; turn++ {
		a.mu.Lock()
		a.phase = "thinking"
		a.mu.Unlock()
		raw, err := call(ctx, sys, a.cause, actor.ActorID(a.start.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{ModelRef: parseModel(a.start.Model), SystemPrompt: artifact.SystemPrompt, Messages: history, Tools: tools})
		if err != nil {
			state := "confirmed"
			kind := "llm_failed"
			if errors.Is(err, context.Canceled) {
				kind = "cancelled"
				state = "confirmed_stopped"
				l.report(sys, a, "cancelled", through, nil, kind, err.Error(), state)
				return
			}
			l.report(sys, a, "failed", through, nil, kind, err.Error(), state)
			return
		}
		var generated llmproto.GenerateResponse
		if err := json.Unmarshal(raw, &generated); err != nil {
			l.report(sys, a, "failed", through, nil, "llm_invalid", err.Error(), "confirmed")
			return
		}
		history = append(history, generated.Message)
		a.setHistory(history)
		if historySize(history) > agentloop.MaxHistoryBytes {
			l.report(sys, a, "failed", through, nil, "context_limit", "Pi history exceeded the phase-one recovery limit before tool execution", "confirmed")
			return
		}
		calls, text, err := assistantParts(generated.Message)
		if err != nil {
			l.report(sys, a, "failed", through, nil, "llm_invalid", err.Error(), "confirmed")
			return
		}
		if len(calls) == 0 {
			resultText, truncated := boundedResultText(text)
			result, _ := json.Marshal(map[string]any{"text": resultText, "text_truncated": truncated, "message": generated.Message, "context_artifact_id": artifact.ArtifactID, "provider": generated.Provider, "model": generated.Model})
			l.report(sys, a, "completed", through, result, "", "", "confirmed")
			return
		}
		if a.start.WorkspaceActor == "" {
			l.report(sys, a, "failed", through, nil, "tool_unavailable", "model requested a tool but this looper has no workspace actor", "not_started")
			return
		}
		a.mu.Lock()
		a.phase = "acting"
		a.mu.Unlock()
		for _, tc := range calls {
			word := toolWord(tc.Name)
			if word == "" {
				history = append(history, toolResult(tc, true, "unknown tool "+tc.Name))
				a.setHistory(history)
				if historySize(history) > agentloop.MaxHistoryBytes {
					l.report(sys, a, "failed", through, nil, "context_limit", "Pi history exceeded the phase-one recovery limit", "confirmed")
					return
				}
				continue
			}
			toolRaw, callErr := call(ctx, sys, a.cause, actor.ActorID(a.start.WorkspaceActor), word, json.RawMessage(tc.Arguments))
			if callErr != nil {
				if errors.Is(callErr, context.Canceled) {
					l.report(sys, a, "cancelled", through, nil, "cancelled", callErr.Error(), "confirmed_stopped")
					return
				}
				history = append(history, toolResult(tc, true, callErr.Error()))
				a.setHistory(history)
				if historySize(history) > agentloop.MaxHistoryBytes {
					l.report(sys, a, "failed", through, nil, "context_limit", "Pi history exceeded the phase-one recovery limit", "confirmed")
					return
				}
				continue
			}
			history = append(history, toolResultRaw(tc, toolRaw))
			a.setHistory(history)
			if historySize(history) > agentloop.MaxHistoryBytes {
				l.report(sys, a, "failed", through, nil, "context_limit", "Pi history exceeded the phase-one recovery limit", "confirmed")
				return
			}
		}
	}
	l.report(sys, a, "failed", through, nil, "turn_limit", "Agent loop reached max_turns", "confirmed")
}

func boundedResultText(text string) (string, bool) {
	if len(text) <= maxResultTextBytes {
		return text, false
	}
	cut := maxResultTextBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut], true
}

type toolCall struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func assistantParts(raw json.RawMessage) ([]toolCall, string, error) {
	var m struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || m.Role != "assistant" || m.Content == nil {
		return nil, "", errors.New("LLM message must be an assistant object with content blocks")
	}
	var calls []toolCall
	var text string
	for _, block := range m.Content {
		var head struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(block, &head) != nil || head.Type == "" {
			return nil, "", errors.New("LLM message contains an invalid content block")
		}
		if head.Type == "text" {
			text += head.Text
		}
		if head.Type == "toolCall" {
			var tc toolCall
			var arguments map[string]json.RawMessage
			if json.Unmarshal(block, &tc) != nil || strings.TrimSpace(tc.ID) == "" || strings.TrimSpace(tc.Name) == "" || json.Unmarshal(tc.Arguments, &arguments) != nil || arguments == nil {
				return nil, "", errors.New("LLM tool call requires id, name, and object arguments")
			}
			calls = append(calls, tc)
		}
	}
	return calls, text, nil
}
func toolResult(tc toolCall, isErr bool, text string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": []map[string]any{{"type": "text", "text": text}}, "isError": isErr, "timestamp": time.Now().UnixMilli()})
	return raw
}
func toolResultRaw(tc toolCall, raw json.RawMessage) json.RawMessage {
	var result struct {
		Content        []json.RawMessage `json:"content"`
		Details        json.RawMessage   `json:"details"`
		Usage          json.RawMessage   `json:"usage"`
		AddedToolNames []string          `json:"addedToolNames"`
	}
	if json.Unmarshal(raw, &result) != nil || len(result.Content) == 0 {
		return toolResult(tc, false, string(raw))
	}
	message := map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": result.Content, "isError": false, "timestamp": time.Now().UnixMilli()}
	if len(result.Details) > 0 && string(result.Details) != "null" {
		message["details"] = result.Details
	}
	if len(result.Usage) > 0 && string(result.Usage) != "null" {
		message["usage"] = result.Usage
	}
	if len(result.AddedToolNames) > 0 {
		message["addedToolNames"] = result.AddedToolNames
	}
	out, _ := json.Marshal(message)
	return out
}
func toolWord(name string) string {
	switch name {
	case "read":
		return workspaceproto.TypeRead
	case "write":
		return workspaceproto.TypeWrite
	case "edit":
		return workspaceproto.TypeEdit
	case "bash":
		return workspaceproto.TypeBash
	}
	return ""
}
func toolDefinitions(enabled bool) []json.RawMessage {
	if !enabled {
		return nil
	}
	specs := []struct{ name, desc, schema string }{{"read", "Read a file from the workspace.", workspaceproto.ReadInputSchema}, {"write", "Write a file in the workspace.", workspaceproto.WriteInputSchema}, {"edit", "Edit exact unique blocks in one file.", workspaceproto.EditInputSchema}, {"bash", "Execute a bash command in the workspace.", workspaceproto.BashInputSchema}}
	out := make([]json.RawMessage, 0, len(specs))
	for _, s := range specs {
		out = append(out, json.RawMessage(fmt.Sprintf(`{"name":%q,"description":%q,"parameters":%s}`, s.name, s.desc, s.schema)))
	}
	return out
}
func parseModel(value string) llmproto.ModelRef {
	for i := 0; i < len(value); i++ {
		if value[i] == '/' {
			return llmproto.ModelRef{Provider: value[:i], Model: value[i+1:]}
		}
	}
	return llmproto.ModelRef{Model: value}
}
func call(ctx context.Context, sys actorbase.Sys, cause message.Cause, target actor.ActorID, typ string, payload any) (json.RawMessage, error) {
	pd, err := sys.Call(cause, target, typ, payload)
	if err != nil {
		return nil, err
	}
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for range pd.Progress() {
			// Progress is already a ledger fact. Phase one does not reinterpret
			// provider-specific stream events as Controller state, but consuming
			// them prevents bounded delivery from backpressuring the endpoint.
		}
	}()
	msg, err := pd.Wait(ctx, 0)
	if err != nil {
		_ = pd.Cancel()
		<-progressDone
		return nil, err
	}
	<-progressDone
	var state struct {
		Status string `json:"status"`
		message.Failure
	}
	if json.Unmarshal(msg.Payload, &state) != nil || state.Status != "completed" {
		if state.ErrorCode == "" {
			state.ErrorCode = "call_failed"
		}
		return nil, fmt.Errorf("%s: %s", state.ErrorCode, state.Detail)
	}
	return append(json.RawMessage(nil), msg.Payload...), nil
}
func (l *looper) report(sys actorbase.Sys, a *assignment, state string, through int64, result json.RawMessage, code, detail, execution string) {
	a.mu.Lock()
	a.phase = state
	history := append([]json.RawMessage(nil), a.history...)
	a.mu.Unlock()
	if historySize(history) > agentloop.MaxHistoryBytes {
		history = nil
		if state == "completed" {
			state, result, code, detail = "failed", nil, "context_limit", "Pi history exceeded the phase-one recovery limit"
		}
	}
	// A terminal report releases the lane before it is published. The
	// Controller may schedule a successor as soon as it adopts this report; if
	// the old assignment remained in active until the goroutine returned, that
	// successor could be rejected by a lane that was already logically free.
	if state != "accepted" {
		l.mu.Lock()
		if l.active[string(a.start.WorkID)] == a {
			delete(l.active, string(a.start.WorkID))
		}
		l.mu.Unlock()
	}
	payload := agentloop.ReportRequest{WorkID: a.start.WorkID, AssignmentID: a.start.AssignmentID, State: state, ConsumedThrough: through, Result: result, ErrorCode: code, Detail: detail, ExecutionState: execution, History: history}
	_, _ = sys.Post(behavior.RequestSpec{Cause: a.cause, Type: agentloop.TypeReport, Audience: message.Audience{actor.ActorID(a.start.ControllerActor)}, Payload: mustJSON(payload)})
}

func historySize(history []json.RawMessage) int {
	total := 0
	for _, item := range history {
		total += len(item)
	}
	return total
}

func (l *looper) reportRejected(sys actorbase.Sys, msg actorbase.Msg, req agentloop.StartRequest, code, detail string) {
	payload := agentloop.ReportRequest{WorkID: req.WorkID, AssignmentID: req.AssignmentID, State: "failed", ErrorCode: code, Detail: detail, ExecutionState: "not_started"}
	_, _ = sys.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: agentloop.TypeReport, Audience: message.Audience{msg.Sender.ID}, Payload: mustJSON(payload)})
}
func (a *assignment) setHistory(history []json.RawMessage) {
	a.mu.Lock()
	a.history = append([]json.RawMessage(nil), history...)
	a.mu.Unlock()
}
func mustJSON(v any) json.RawMessage { raw, _ := json.Marshal(v); return raw }
func stateForError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "confirmed_stopped"
	}
	return "confirmed"
}
