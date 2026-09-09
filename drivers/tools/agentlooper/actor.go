package agentlooper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
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
const channelCallWord = "channel.call"

const (
	defaultToolResultMaxLines = 2000
	defaultToolResultMaxBytes = 50 << 10
	defaultToolImageMaxBytes  = 3 << 20
)

var validToolName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)

type resolvedTool struct {
	Name       string
	Actor      string
	Word       string
	Definition json.RawMessage
}

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
	start    agentloop.StartRequest
	cause    message.Cause
	cancel   context.CancelFunc
	mu       sync.Mutex
	inputs   []agentloop.Input
	phase    string
	history  []json.RawMessage
	closed   bool
	consumed int
	controls []agentloop.ControlResult
}
type looper struct {
	cfg           Config
	mu            sync.Mutex
	active        map[string]*assignment
	finished      map[string]*assignment
	finishedOrder []string
	wg            sync.WaitGroup
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
	old := l.active[req.AssignmentID]
	if old == nil {
		old = l.finished[req.AssignmentID]
	}
	if old != nil {
		same := string(mustJSON(old.start)) == string(mustJSON(req))
		l.mu.Unlock()
		if !same {
			_, _ = sys.Fail(msg, "assignment_conflict", "assignment id reused with different request")
			return
		}
		old.mu.Lock()
		closed := old.closed
		old.mu.Unlock()
		disposition := "already_accepted"
		if closed {
			disposition = "already_finished"
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": disposition, "assignment_id": req.AssignmentID})
		return
	}

	if len(l.active) >= l.cfg.MaxAssignments {
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "capacity", "looper has reached max_assignments")
		l.reportRejected(sys, msg, req, "capacity", "looper has reached max_assignments")
		return
	}
	if req.ToolTimeoutMS < 0 || req.ExecutionTimeoutMS < 0 {
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "invalid_args", "timeouts must be positive when configured")
		return
	}
	duration := 30 * time.Minute
	if req.ExecutionTimeoutMS > 0 {
		duration = time.Duration(req.ExecutionTimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(sys.Life(), duration)
	a := &assignment{start: req, cause: msg.Cause(), cancel: cancel, inputs: append([]agentloop.Input(nil), req.Inputs...), phase: "accepted"}
	l.active[req.AssignmentID] = a
	l.wg.Add(1)
	l.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted", "work_id": req.WorkID, "assignment_id": req.AssignmentID})
	l.report(sys, a, "accepted", 0, nil, "", "", "confirmed_running")
	go func() {
		defer l.wg.Done()
		defer cancel()
		l.drive(ctx, sys, a)
		l.mu.Lock()
		if l.active[req.AssignmentID] == a {
			delete(l.active, req.AssignmentID)
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
	a := l.active[req.AssignmentID]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.ViewID != req.ViewID {
		_, _ = sys.Fail(msg, "operation_mismatch", "input targets a stale assignment")
		return
	}
	decision, err := a.acceptInput(req)
	if err != nil {
		_, _ = sys.Fail(msg, "operation_conflict", err.Error())
		return
	}
	_, _ = sys.Reply(msg, decision)
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
	a := l.active[req.AssignmentID]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "already_stopped", "work_id": req.WorkID})
		return
	}
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.ViewID != req.ViewID {
		_, _ = sys.Fail(msg, "operation_mismatch", "stop targets a stale assignment")
		return
	}
	a.mu.Lock()
	a.closed = true
	a.cancel()
	a.mu.Unlock()
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
	a := l.active[req.AssignmentID]
	if a == nil {
		a = l.finished[req.AssignmentID]
	}
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.ViewID != req.ViewID {
		_, _ = sys.Fail(msg, "operation_mismatch", "inspect targets a different session or incarnation")
		return
	}
	a.mu.Lock()
	phase, n := a.phase, len(a.inputs)
	controls := append([]agentloop.ControlResult(nil), a.controls...)
	consumed := a.consumed
	a.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"work_id": req.WorkID, "assignment_id": a.start.AssignmentID, "phase": phase, "input_count": n, "controls": controls, "consumed_count": consumed})
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
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				a.mu.Lock()
				closed := a.closed
				a.mu.Unlock()
				if closed {
					return
				}
				_, _ = sys.Post(behavior.RequestSpec{Cause: a.cause, Type: agentloop.TypeReport, Audience: message.Audience{actor.ActorID(a.start.ControllerActor)}, Payload: mustJSON(agentloop.ReportRequest{WorkID: a.start.WorkID, AssignmentID: a.start.AssignmentID, ViewID: a.start.ViewID, ContextVersion: a.start.ContextVersion, State: "processing"})})
			}
		}
	}()
	defer func() { stopHeartbeat(); <-heartbeatDone }()
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
		l.report(sys, a, "failed", 0, nil, "context_failed", err.Error(), stateForError(err))
		return
	}
	var artifact contextproto.Artifact
	if err := json.Unmarshal(contextRaw, &artifact); err != nil {
		l.report(sys, a, "failed", 0, nil, "context_invalid", err.Error(), "confirmed")
		return
	}
	history := append([]json.RawMessage(nil), artifact.Messages...)
	if err := validateHistory(history); err != nil {
		l.report(sys, a, "failed", 0, nil, "context_invalid", err.Error(), "confirmed")
		return
	}
	a.mu.Lock()
	a.consumed = len(inputs)
	a.mu.Unlock()
	a.setHistory(history)
	turns := a.start.MaxTurns
	if turns <= 0 {
		turns = 12
	}
	tools, err := resolveTools(ctx, sys, a)
	if err != nil {
		l.report(sys, a, "failed", through, nil, "tool_discovery_failed", err.Error(), stateForError(err))
		return
	}
	definitions := make([]json.RawMessage, 0, len(tools))
	targets := make(map[string]resolvedTool, len(tools))
	for _, tool := range tools {
		definitions = append(definitions, tool.Definition)
		targets[tool.Name] = tool
	}
	for turn := 0; turn < turns; turn++ {
		if ctx.Err() != nil {
			l.report(sys, a, "cancelled", through, nil, "cancelled", ctx.Err().Error(), "confirmed_stopped")
			return
		}
		pending := a.pendingInputs()
		if len(pending) > 0 {
			built, buildErr := call(ctx, sys, a.cause, actor.ActorID(a.start.ContextActor), contextproto.TypeBuild, contextproto.BuildRequest{WorkID: a.start.WorkID, AssignmentID: a.start.AssignmentID, Inputs: pending, Prior: history})
			var next contextproto.Artifact
			if buildErr != nil || json.Unmarshal(built, &next) != nil {
				l.report(sys, a, "failed", through, nil, "context_failed", "cannot build accepted control input", "confirmed")
				return
			}
			history = append([]json.RawMessage(nil), next.Messages...)
			a.mu.Lock()
			a.consumed += len(pending)
			a.mu.Unlock()
			for _, in := range pending {
				if in.Seq > through {
					through = in.Seq
				}
			}
			a.setHistory(history)
		}
		if err := validateHistory(history); err != nil {
			l.report(sys, a, "failed", through, nil, "context_invalid", err.Error(), "confirmed")
			return
		}
		a.mu.Lock()
		a.phase = "thinking"
		a.mu.Unlock()
		raw, err := call(ctx, sys, a.cause, actor.ActorID(a.start.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{ModelRef: parseModel(a.start.Model), SystemPrompt: artifact.SystemPrompt, Messages: history, Tools: definitions})
		if err != nil {
			state := "confirmed"
			kind := "llm_failed"
			var failure *callFailure
			if errors.As(err, &failure) && failure.Code != "" {
				kind = failure.Code
			}
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
		calls, text, err := assistantParts(generated.Message)
		if err != nil {
			l.report(sys, a, "failed", through, nil, "invalid_model_response", err.Error(), "confirmed")
			return
		}
		reason := stopReason(generated.Message)
		if reason == "error" || reason == "aborted" {
			l.report(sys, a, "failed", through, nil, "provider_error", "provider did not deliver a complete successful assistant", "confirmed")
			return
		}
		history = append(history, generated.Message)
		a.setHistory(history)
		if len(calls) == 0 {
			if !a.seal() {
				continue
			}
			resultText, truncated := boundedResultText(text)
			result, _ := json.Marshal(map[string]any{"text": resultText, "text_truncated": truncated, "message": generated.Message, "context_artifact_id": artifact.ArtifactID, "provider": generated.Provider, "model": generated.Model})
			l.report(sys, a, "completed", through, result, "", "", "confirmed")
			return
		}
		a.mu.Lock()
		a.phase = "acting"
		a.mu.Unlock()
		for _, tc := range calls {
			if reason == "length" {
				history = append(history, toolResult(tc, true, "Looper: model output truncated; this tool call was not executed"))
				continue
			}
			if ctx.Err() != nil {
				history = append(history, toolResult(tc, true, "Looper: execution cancelled before this tool was dispatched"))
				continue
			}
			if !validArguments(tc.Arguments) {
				history = append(history, toolResult(tc, true, "Looper: invalid_args; arguments must be an object; tool was not executed"))
				continue
			}
			tool, ok := targets[tc.Name]
			if !ok {
				history = append(history, toolResult(tc, true, "unknown tool "+tc.Name))
				a.setHistory(history)
				continue
			}
			toolTimeout := 2 * time.Minute
			if a.start.ToolTimeoutMS > 0 {
				toolTimeout = time.Duration(a.start.ToolTimeoutMS) * time.Millisecond
			}
			toolCtx, cancelTool := context.WithTimeout(ctx, toolTimeout)
			toolRaw, callErr := call(toolCtx, sys, a.cause, actor.ActorID(tool.Actor), tool.Word, json.RawMessage(tc.Arguments))
			cancelTool()
			if callErr != nil {
				detail := callErr.Error()
				if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
					detail = "Looper: no tool result received; external execution/effects unknown: " + detail
				}
				result := toolResult(tc, true, detail)
				var failure *callFailure
				if errors.As(callErr, &failure) {
					var fields map[string]json.RawMessage
					_ = json.Unmarshal(result, &fields)
					fields["details"] = mustRaw(map[string]any{"source": "looper_wait", "request_id": failure.RequestID, "error_code": failure.Code, "external_effects_unknown": errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded)})
					result = mustRaw(fields)
				}
				history = append(history, result)
				a.setHistory(history)
				continue
			}
			history = append(history, l.toolResult(ctx, sys, a, tc, tool, toolRaw))
			a.setHistory(history)
		}
		a.setHistory(history)
		if ctx.Err() != nil {
			l.report(sys, a, "cancelled", through, nil, "cancelled", "local execution stopped; dispatched tool effects may be unknown", "confirmed_stopped")
			return
		}
		if historySize(history) > agentloop.MaxHistoryBytes {
			l.report(sys, a, "failed", through, nil, "context_limit", "history limit exceeded after closing tool batch", "confirmed")
			return
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
	seen := map[string]bool{}
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
			if json.Unmarshal(block, &tc) != nil || strings.TrimSpace(tc.ID) == "" || strings.TrimSpace(tc.Name) == "" || seen[tc.ID] {
				return nil, "", errors.New("LLM tool call requires a unique nonempty id and name")
			}
			seen[tc.ID] = true
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

func (l *looper) toolResult(ctx context.Context, sys actorbase.Sys, a *assignment, tc toolCall, tool resolvedTool, raw json.RawMessage) json.RawMessage {
	maxLines, maxBytes, maxImage := a.start.ToolResultMaxLines, a.start.ToolResultMaxBytes, a.start.ToolImageMaxBytes
	if maxLines <= 0 {
		maxLines = defaultToolResultMaxLines
	}
	if maxBytes <= 0 {
		maxBytes = defaultToolResultMaxBytes
	}
	if maxImage <= 0 {
		maxImage = defaultToolImageMaxBytes
	}
	var result struct {
		Content        []json.RawMessage `json:"content"`
		Details        json.RawMessage   `json:"details"`
		Usage          json.RawMessage   `json:"usage"`
		AddedToolNames []string          `json:"addedToolNames"`
		IsError        bool              `json:"isError"`
	}
	piShape := json.Unmarshal(raw, &result) == nil && len(result.Content) > 0
	if !piShape {
		result.Content = []json.RawMessage{mustRaw(map[string]any{"type": "text", "text": string(raw)})}
	}
	var fullText strings.Builder
	imageBytes := 0
	for _, block := range result.Content {
		var value struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Data     string `json:"data"`
			MimeType string `json:"mimeType"`
		}
		if err := json.Unmarshal(block, &value); err != nil {
			return toolResult(tc, true, "tool returned an invalid content block: "+err.Error())
		}
		switch value.Type {
		case "text":
			fullText.WriteString(value.Text)
		case "image":
			if value.MimeType != "image/png" && value.MimeType != "image/jpeg" && value.MimeType != "image/gif" && value.MimeType != "image/webp" {
				return toolResult(tc, true, "tool returned an unsupported image MIME type")
			}
			if len(value.Data) > (maxImage-imageBytes+2)/3*4+4 {
				return toolResult(tc, true, fmt.Sprintf("tool image is invalid or exceeds %d bytes", maxImage))
			}
			decoded, err := base64.StdEncoding.DecodeString(value.Data)
			imageBytes += len(decoded)
			if err != nil || imageBytes > maxImage {
				return toolResult(tc, true, fmt.Sprintf("tool image is invalid or exceeds %d bytes", maxImage))
			}
		default:
			return toolResult(tc, true, "tool returned an unsupported content block type")
		}
	}
	full := fullText.String()
	tail := tool.Word == workspaceproto.TypeBash || tool.Word == workspaceproto.TypePowerShell
	excerpt, truncated := boundedToolText(full, maxLines, maxBytes, tail)
	if !truncated {
		out := toolResultRaw(tc, raw)
		if !piShape {
			return toolResult(tc, result.IsError, full)
		}
		if result.IsError {
			var message map[string]any
			if json.Unmarshal(out, &message) == nil {
				message["isError"] = true
				return mustRaw(message)
			}
		}
		return out
	}
	if a.start.WorkspaceActor == "" {
		return toolResult(tc, true, fmt.Sprintf("tool output exceeds %d lines or %d bytes and no workspace output store is configured", maxLines, maxBytes))
	}
	path := ".atoll/tool-results/" + uuid.NewString() + ".txt"
	_, err := call(ctx, sys, a.cause, actor.ActorID(a.start.WorkspaceActor), workspaceproto.TypeWrite, workspaceproto.WriteRequest{Path: path, Content: full})
	if err != nil {
		return toolResult(tc, true, "tool output exceeded the configured limit and the complete output could not be saved: "+err.Error())
	}
	noticePrefix := " "
	excerptLines := maxLines
	if maxLines > 1 {
		noticePrefix = "\n"
		excerptLines--
	}
	notice := fmt.Sprintf("%s[Output truncated; complete output saved to %s]", noticePrefix, path)
	excerptBytes := maxBytes - len(notice)
	if excerptLines < 1 {
		excerptLines = 1
	}
	if excerptBytes < 0 {
		excerptBytes = 0
	}
	excerpt, _ = boundedToolText(full, excerptLines, excerptBytes, tail)
	content := make([]json.RawMessage, 0, len(result.Content))
	textAdded := false
	for _, block := range result.Content {
		var head struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(block, &head)
		if head.Type == "text" {
			if !textAdded {
				content = append(content, mustRaw(map[string]any{"type": "text", "text": excerpt + notice}))
				textAdded = true
			}
			continue
		}
		content = append(content, block)
	}
	details := map[string]any{}
	if len(result.Details) > 0 && string(result.Details) != "null" {
		if json.Unmarshal(result.Details, &details) != nil {
			details = map[string]any{"tool_details": result.Details}
		}
	}
	details["atoll_output"] = map[string]any{"truncated": true, "path": path, "workspace_actor": a.start.WorkspaceActor, "retained": map[bool]string{true: "tail", false: "head"}[tail]}
	message := map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": content, "details": details, "isError": result.IsError, "timestamp": time.Now().UnixMilli()}
	if len(result.Usage) > 0 && string(result.Usage) != "null" {
		message["usage"] = result.Usage
	}
	if len(result.AddedToolNames) > 0 {
		message["addedToolNames"] = result.AddedToolNames
	}
	return mustRaw(message)
}

func boundedToolText(text string, maxLines, maxBytes int, tail bool) (string, bool) {
	if maxLines < 1 || maxBytes < 1 {
		return "", text != ""
	}
	if len(text) <= maxBytes && strings.Count(text, "\n") < maxLines {
		return text, false
	}
	if tail {
		start := len(text) - maxBytes
		if start < 0 {
			start = 0
		}
		for start < len(text) && !utf8.RuneStart(text[start]) {
			start++
		}
		part := text[start:]
		for strings.Count(part, "\n") >= maxLines {
			if i := strings.IndexByte(part, '\n'); i >= 0 {
				part = part[i+1:]
			} else {
				break
			}
		}
		return part, true
	}
	end := len(text)
	if end > maxBytes {
		end = maxBytes
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
	}
	part := text[:end]
	if strings.Count(part, "\n") >= maxLines {
		position := 0
		for range maxLines {
			i := strings.IndexByte(part[position:], '\n')
			if i < 0 {
				break
			}
			position += i + 1
		}
		part = strings.TrimSuffix(part[:position], "\n")
	}
	return part, true
}

func mustRaw(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}
func configuredTools(start agentloop.StartRequest) []agentloop.ToolBinding {
	if start.Tools != nil {
		return append([]agentloop.ToolBinding(nil), (*start.Tools)...)
	}
	var tools []agentloop.ToolBinding
	if start.WorkspaceActor != "" {
		for _, item := range []struct{ name, word string }{
			{"read", workspaceproto.TypeRead}, {"write", workspaceproto.TypeWrite},
			{"edit", workspaceproto.TypeEdit}, {"bash", workspaceproto.TypeBash},
		} {
			tools = append(tools, agentloop.ToolBinding{Name: item.name, Actor: start.WorkspaceActor, Word: item.word})
		}
	}
	if start.HostActor != "" {
		for _, item := range []struct{ name, word string }{
			{"channel_call", channelCallWord}, {"channel_post", "channel.post"}, {"channel_emit", "channel.emit"},
		} {
			tools = append(tools, agentloop.ToolBinding{Name: item.name, Actor: start.HostActor, Word: item.word})
		}
	}
	return tools
}

func resolveTools(ctx context.Context, sys actorbase.Sys, a *assignment) ([]resolvedTool, error) {
	bindings := configuredTools(a.start)
	seen := make(map[string]struct{}, len(bindings))
	manifests := make(map[string]introspect.Describe)
	out := make([]resolvedTool, 0, len(bindings))
	for _, binding := range bindings {
		if !validToolName.MatchString(binding.Name) || strings.TrimSpace(binding.Actor) == "" || strings.TrimSpace(binding.Word) == "" {
			return nil, fmt.Errorf("invalid tool binding %q", binding.Name)
		}
		if _, exists := seen[binding.Name]; exists {
			return nil, fmt.Errorf("duplicate tool name %q", binding.Name)
		}
		seen[binding.Name] = struct{}{}
		describe, ok := manifests[binding.Actor]
		if !ok {
			raw, err := call(ctx, sys, a.cause, actor.ActorID(binding.Actor), introspect.QueryDescribe, introspect.DescribeRequest{})
			if err != nil {
				return nil, fmt.Errorf("describe tool actor %q: %w", binding.Actor, err)
			}
			if err := json.Unmarshal(raw, &describe); err != nil {
				return nil, fmt.Errorf("describe tool actor %q returned invalid manifest: %w", binding.Actor, err)
			}
			manifests[binding.Actor] = describe
		}
		spec, ok := describe.Words[binding.Word]
		if !ok {
			return nil, fmt.Errorf("tool actor %q declares no request word %q", binding.Actor, binding.Word)
		}
		if err := validInputSchema(spec.InputSchema); err != nil {
			return nil, fmt.Errorf("tool %q word %q has invalid input schema: %w", binding.Name, binding.Word, err)
		}
		definition, err := json.Marshal(map[string]any{"name": binding.Name, "description": spec.Description, "parameters": spec.InputSchema})
		if err != nil {
			return nil, err
		}
		out = append(out, resolvedTool{Name: binding.Name, Actor: binding.Actor, Word: binding.Word, Definition: definition})
	}
	return out, nil
}

func validInputSchema(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return errors.New("schema is missing or invalid JSON")
	}
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(raw, &schema); err != nil || schema == nil {
		return errors.New("schema must be a JSON object")
	}
	if err := validateSchemaShape(schema); err != nil {
		return err
	}
	var typ string
	if rawType, ok := schema["type"]; ok {
		if err := json.Unmarshal(rawType, &typ); err != nil || typ != "object" {
			return errors.New("schema must describe an object")
		}
		return nil
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		raw, ok := schema[keyword]
		if !ok {
			continue
		}
		var alternatives []json.RawMessage
		_ = json.Unmarshal(raw, &alternatives)
		for _, alternative := range alternatives {
			if err := validInputSchema(alternative); err != nil {
				return fmt.Errorf("schema %s alternative: %w", keyword, err)
			}
		}
		return nil
	}
	return errors.New("schema must describe an object or object alternatives")
}

func validateSchemaShape(schema map[string]json.RawMessage) error {
	if raw, ok := schema["type"]; ok {
		var typ string
		if json.Unmarshal(raw, &typ) != nil || typ == "" {
			return errors.New("schema type must be a non-empty string")
		}
	}
	if raw, ok := schema["properties"]; ok {
		var properties map[string]json.RawMessage
		if json.Unmarshal(raw, &properties) != nil || properties == nil {
			return errors.New("schema properties must be an object")
		}
		for name, child := range properties {
			var nested map[string]json.RawMessage
			if json.Unmarshal(child, &nested) != nil || nested == nil {
				return fmt.Errorf("schema property %q must be an object", name)
			}
			if err := validateSchemaShape(nested); err != nil {
				return fmt.Errorf("schema property %q: %w", name, err)
			}
		}
	}
	if raw, ok := schema["required"]; ok {
		var required []string
		if json.Unmarshal(raw, &required) != nil {
			return errors.New("schema required must be a string array")
		}
	}
	if raw, ok := schema["items"]; ok {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw, &nested) != nil || nested == nil {
			return errors.New("schema items must be an object")
		}
		if err := validateSchemaShape(nested); err != nil {
			return fmt.Errorf("schema items: %w", err)
		}
	}
	if raw, ok := schema["additionalProperties"]; ok {
		var allowed bool
		if json.Unmarshal(raw, &allowed) != nil {
			var nested map[string]json.RawMessage
			if json.Unmarshal(raw, &nested) != nil || nested == nil {
				return errors.New("schema additionalProperties must be boolean or object")
			}
			if err := validateSchemaShape(nested); err != nil {
				return fmt.Errorf("schema additionalProperties: %w", err)
			}
		}
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		raw, ok := schema[keyword]
		if !ok {
			continue
		}
		var alternatives []map[string]json.RawMessage
		if json.Unmarshal(raw, &alternatives) != nil || len(alternatives) == 0 {
			return fmt.Errorf("schema %s must be a non-empty object array", keyword)
		}
		for _, alternative := range alternatives {
			if err := validateSchemaShape(alternative); err != nil {
				return fmt.Errorf("schema %s: %w", keyword, err)
			}
		}
	}
	return nil
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pd, err := sys.Call(cause, target, typ, payload)
	if err != nil {
		return nil, err
	}
	progressDone := make(chan struct{})
	drainCtx, stopDrain := context.WithCancel(ctx)
	defer stopDrain()
	go func() {
		defer close(progressDone)
		for {
			select {
			case _, ok := <-pd.Progress():
				if !ok {
					return
				}
			case <-drainCtx.Done():
				return
			}
		}
	}()
	msg, err := pd.Wait(ctx, 0)
	stopDrain()
	if err != nil {
		_ = pd.Cancel()
		<-progressDone
		code := "call_failed"
		if errors.Is(err, context.Canceled) {
			code = "cancelled"
		}
		if errors.Is(err, context.DeadlineExceeded) {
			code = "deadline_exceeded"
		}
		return nil, &callFailure{Code: code, Detail: err.Error(), RequestID: string(pd.RequestID()), cause: err}
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
		return nil, &callFailure{Code: state.ErrorCode, Detail: state.Detail, RequestID: string(pd.RequestID())}
	}
	return append(json.RawMessage(nil), msg.Payload...), nil
}
func (l *looper) report(sys actorbase.Sys, a *assignment, state string, through int64, result json.RawMessage, code, detail, execution string) {
	a.mu.Lock()
	a.phase = state
	if state != "accepted" {
		a.closed = true
	}
	history := append([]json.RawMessage(nil), a.history...)
	controls := append([]agentloop.ControlResult(nil), a.controls...)
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
		if l.active[a.start.AssignmentID] == a {
			delete(l.active, a.start.AssignmentID)
		}
		if l.finished == nil {
			l.finished = map[string]*assignment{}
		}
		l.finished[a.start.AssignmentID] = a
		l.finishedOrder = append(l.finishedOrder, a.start.AssignmentID)
		for len(l.finishedOrder) > 256 {
			delete(l.finished, l.finishedOrder[0])
			l.finishedOrder = l.finishedOrder[1:]
		}
		l.mu.Unlock()
	}
	payload := agentloop.ReportRequest{WorkID: a.start.WorkID, AssignmentID: a.start.AssignmentID, ViewID: a.start.ViewID, ContextVersion: a.start.ContextVersion, Controls: controls, State: state, ConsumedThrough: through, Result: result, ErrorCode: code, Detail: detail, ExecutionState: execution, History: history}
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
