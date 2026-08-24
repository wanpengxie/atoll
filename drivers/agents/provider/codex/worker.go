package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/emit"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
	"github.com/wanpengxie/atoll/lib/metatool"
)

type connection struct {
	process *childProcess
	rpc     *rpcClient
	retired atomic.Bool
}

func (c *connection) Retire() {
	if c != nil && !c.retired.Swap(true) {
		c.rpc.retire()
		c.process.stop()
	}
}

type workerPhase uint8

const (
	phaseConstructed workerPhase = iota
	phaseOpening
	phaseReady
	phaseStarting
	phaseActive
	phaseRetiring
	phaseReaped
)

type worker struct {
	cfg     Config
	host    driverproto.WorkerHost
	gate    *emit.Gate
	mu      sync.Mutex
	phase   workerPhase
	conn    *connection
	thread  string
	attempt driverproto.AttemptToken
	target  driverproto.WorkerTurnTarget
	final   map[driverproto.WorkerTurnRef]string
	options driverproto.TurnOptions
	pending driverproto.TurnOptions
	usage   driverproto.TurnUsage
	// sessionModel/sessionEffort are the ACTUAL defaults codex reported in the
	// thread/start|resume response (model + reasoningEffort). When the decl
	// configures nothing, turns run on these — usage accounting reports them
	// instead of "", so the ledger always names what the turn actually ran on.
	sessionModel  string
	sessionEffort string
	leases        sync.WaitGroup
	reaped        chan struct{}
	retireOnce    sync.Once
	surface       toolsurface.Surface
}

func newWorker(cfg Config, host driverproto.WorkerHost) *worker {
	return &worker{cfg: cfg, host: host, gate: emit.New(host.Events()), phase: phaseConstructed, final: map[driverproto.WorkerTurnRef]string{}, reaped: make(chan struct{})}
}

func (w *worker) begin(allowed ...workerPhase) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range allowed {
		if w.phase == p {
			w.leases.Add(1)
			return true
		}
	}
	return false
}
func (w *worker) end() { w.leases.Done() }

// Open registers all ownership before writing and returns after the initial
// initialize frame has been physically submitted. Native replies are mapped
// back through the one EventSink stream.
func (w *worker) Open(ctx context.Context, req driverproto.OpenRequest) {
	if !w.begin(phaseConstructed) {
		return
	}
	defer w.end()
	w.mu.Lock()
	w.phase = phaseOpening
	w.options = req.Options
	w.pending.Effort = req.Options.Effort
	w.mu.Unlock()
	if w.host == nil || w.host.Tools() == nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: "tool host unavailable", Disposition: driverproto.RetireWorker})
		return
	}
	surface, err := toolsurface.Assemble(w.host.Tools().Catalog(), toolsurface.Codex, w.cfg.Situation)
	if err != nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	w.mu.Lock()
	w.surface = surface
	w.mu.Unlock()
	p, err := w.cfg.processFactory(ctx, w.cfg)
	if err != nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	c := &connection{process: p}
	c.rpc = newRPC(p)
	c.rpc.onNotification = func(method string, params json.RawMessage) {
		if !c.retired.Load() {
			w.notification(c, method, params)
		}
	}
	c.rpc.onRequest = func(method string, params json.RawMessage) func() (any, *rpcError) {
		if c.retired.Load() {
			return func() (any, *rpcError) { return nil, &rpcError{Code: -32601, Message: "connection retired"} }
		}
		return w.prepareServerRequest(c, method, params)
	}
	c.rpc.onClose = func(err error) {
		if !c.retired.Load() {
			w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		}
	}
	w.mu.Lock()
	if w.phase != phaseOpening {
		w.mu.Unlock()
		c.Retire()
		return
	}
	w.conn = c
	w.mu.Unlock()
	c.rpc.start()
	params := map[string]any{"clientInfo": map[string]any{"name": "atoll", "title": "Atoll Codex Agent", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true, "optOutNotificationMethods": deltaNotificationMethods()}}
	if err := c.rpc.callAsync("initialize", params, func(raw json.RawMessage, err error) { w.afterInitialize(c, req.ResumeSeed, raw, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

func (w *worker) afterInitialize(c *connection, seed []byte, raw json.RawMessage, err error) {
	if !w.isOpening(c) {
		return
	}
	if err != nil {
		w.publish(openRejection(err))
		return
	}
	var initialized struct {
		UserAgent string `json:"userAgent"`
	}
	_ = json.Unmarshal(raw, &initialized)
	if err := c.rpc.notify("initialized", map[string]any{}); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		return
	}
	w.mu.Lock()
	model := w.options.Model
	w.mu.Unlock()
	startParams := w.threadStartParams(model, "")
	prompt := w.surface.AppendGuide(w.cfg.Prompt)
	method, params := "thread/start", any(startParams)
	resumeThread, resumeOK := decodeResumeSeed(seed, w.surface.Digest())
	if resumeOK {
		// thread/resume has no dynamicTools field: codex persists a thread's
		// dynamic tools with the thread and restores them on resume.
		// Execution policy is supplied again so an old thread cannot retain a
		// sandbox policy that predates Atoll's current yolo configuration.
		resumeParams := w.threadPolicyParams()
		resumeParams["threadId"] = resumeThread
		resumeParams["excludeTurns"] = true
		if model != "" {
			resumeParams["model"] = model
		}
		if prompt != "" {
			resumeParams["developerInstructions"] = prompt
		}
		method, params = "thread/resume", resumeParams
	}
	if err := c.rpc.callAsync(method, params, func(raw json.RawMessage, err error) { w.afterSession(c, resumeOK, raw, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

func (w *worker) threadStartParams(model, source string) map[string]any {
	params := w.threadPolicyParams()
	if model != "" {
		params["model"] = model
	}
	// The decl-authored prompt is appended to codex's own system prompt as a
	// developer instruction; atoll's tool catalog rides along as dynamic tools
	// on every fresh thread, including agent.new.
	if prompt := w.surface.AppendGuide(w.cfg.Prompt); prompt != "" {
		params["developerInstructions"] = prompt
	}
	if tools := w.dynamicTools(); len(tools) > 0 {
		params["dynamicTools"] = tools
	}
	if source != "" {
		params["sessionStartSource"] = source
	}
	return params
}

func (w *worker) threadPolicyParams() map[string]any {
	return map[string]any{"approvalPolicy": "never", "sandbox": "danger-full-access", "cwd": w.cfg.WorkspaceDir}
}

func (w *worker) afterSession(c *connection, resumed bool, raw json.RawMessage, err error) {
	if !w.isOpening(c) {
		return
	}
	if err != nil {
		if resumed && isInvalidResumeError(err) {
			w.publish(driverproto.OpenRejected{Class: driverproto.FailureResumeInvalid, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		} else {
			w.publish(openRejection(err))
		}
		return
	}
	id := threadIDFrom(raw)
	if id == "" {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: "session response missing thread id", Disposition: driverproto.RetireWorker})
		return
	}
	var configured struct {
		Model           string `json:"model"`
		ReasoningEffort string `json:"reasoningEffort"`
	}
	_ = json.Unmarshal(raw, &configured)
	w.mu.Lock()
	if w.phase != phaseOpening || w.conn != c {
		w.mu.Unlock()
		return
	}
	w.thread, w.phase = id, phaseReady
	w.sessionModel, w.sessionEffort = configured.Model, configured.ReasoningEffort
	w.mu.Unlock()
	if !w.publish(driverproto.SeedUpdated{Value: encodeResumeSeed(id, w.surface.Digest())}) {
		return
	}
	w.publish(driverproto.WorkerReady{})
}

func (w *worker) isOpening(c *connection) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.phase == phaseOpening && w.conn == c
}

func (w *worker) Start(_ context.Context, req driverproto.StartRequest) {
	if !w.begin(phaseReady) {
		return
	}
	defer w.end()
	w.mu.Lock()
	if w.phase != phaseReady || w.conn == nil {
		w.mu.Unlock()
		return
	}
	c, thread := w.conn, w.thread
	w.phase, w.attempt = phaseStarting, req.Attempt
	w.target = driverproto.WorkerTurnTarget{Attempt: req.Attempt}
	if req.Kind == driverproto.TurnSelect {
		selected := req.Options
		if selected.Model == "" {
			selected.Model = w.options.Model
		}
		if selected.Effort == "" {
			selected.Effort = w.options.Effort
		}
		w.options = selected
		w.pending = req.Options
		w.target.Native = driverproto.WorkerTurnRef(fmt.Sprintf("select-%d", req.Attempt))
		target, usage := w.target, w.currentUsageLocked()
		w.phase = phaseActive
		w.mu.Unlock()
		w.publish(driverproto.TurnStarted{Target: target})
		w.mu.Lock()
		if w.attempt == req.Attempt {
			w.attempt, w.target, w.phase = 0, driverproto.WorkerTurnTarget{}, phaseReady
		}
		w.mu.Unlock()
		w.publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, Usage: usage})
		return
	}
	if req.Kind == driverproto.TurnNew {
		target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: driverproto.WorkerTurnRef(fmt.Sprintf("new-%d", req.Attempt))}
		w.target = target
		usage := w.currentUsageLocked()
		params := w.threadStartParams(usage.Model, "clear")
		w.mu.Unlock()
		if err := c.rpc.callAsync("thread/start", params, func(raw json.RawMessage, err error) { w.afterNewResponse(req.Attempt, target, raw, err) }); err != nil {
			w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		}
		return
	}
	pending := w.pending
	if req.Kind == driverproto.TurnChat {
		w.pending = driverproto.TurnOptions{}
	}
	w.mu.Unlock()
	method := "thread/compact/start"
	params := map[string]any{"threadId": thread}
	if req.Kind == driverproto.TurnChat {
		method = "turn/start"
		params["input"] = buildInput(req.Messages, req.Background, w.cfg.Situation)
		if pending.Model != "" {
			params["model"] = pending.Model
		}
		if pending.Effort != "" {
			params["effort"] = pending.Effort
		}
	}
	if err := c.rpc.callAsync(method, params, func(_ json.RawMessage, err error) { w.afterStartResponse(req.Attempt, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

func (w *worker) afterNewResponse(attempt driverproto.AttemptToken, target driverproto.WorkerTurnTarget, raw json.RawMessage, err error) {
	if err != nil {
		w.mu.Lock()
		current := w.attempt == attempt && w.phase == phaseStarting
		if current {
			w.attempt, w.target, w.phase = 0, driverproto.WorkerTurnTarget{}, phaseReady
		}
		w.mu.Unlock()
		if current {
			w.publish(driverproto.SubmissionRejected{Attempt: attempt, Class: driverproto.FailureProvider, Detail: err.Error(), Disposition: driverproto.KeepWorker})
		}
		return
	}
	thread := threadIDFrom(raw)
	if thread == "" {
		w.afterNewResponse(attempt, target, nil, &rpcError{Code: -32603, Message: "new thread response missing thread id"})
		return
	}
	var configured struct {
		Model           string `json:"model"`
		ReasoningEffort string `json:"reasoningEffort"`
	}
	_ = json.Unmarshal(raw, &configured)
	w.mu.Lock()
	if w.attempt != attempt || w.phase != phaseStarting || w.target != target {
		w.mu.Unlock()
		return
	}
	w.thread = thread
	if configured.Model != "" {
		w.sessionModel = configured.Model
	}
	if configured.ReasoningEffort != "" {
		w.sessionEffort = configured.ReasoningEffort
	}
	usage := w.currentUsageLocked()
	usage.ContextTokens = 0
	w.usage = usage
	// thread/start has no effort parameter. Re-apply the actor's current
	// model/effort pair to the first chat turn on the new thread.
	w.pending = w.options
	w.phase = phaseActive
	w.mu.Unlock()
	w.publish(driverproto.TurnStarted{Target: target})
	if !w.publish(driverproto.SeedUpdated{Value: encodeResumeSeed(thread, w.surface.Digest())}) {
		return
	}
	w.mu.Lock()
	if w.attempt == attempt && w.target == target {
		w.attempt, w.target, w.phase = 0, driverproto.WorkerTurnTarget{}, phaseReady
	}
	w.mu.Unlock()
	w.publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, Usage: usage})
}

func (w *worker) currentUsageLocked() driverproto.TurnUsage {
	usage := w.usage
	usage.Model, usage.Effort = w.options.Model, w.options.Effort
	// An option we set on turn/start IS the actual value; when unset, the turn
	// ran on the session defaults codex reported at thread/start|resume.
	if usage.Model == "" {
		usage.Model = w.sessionModel
	}
	if usage.Effort == "" {
		usage.Effort = w.sessionEffort
	}
	return usage
}

func (w *worker) afterStartResponse(attempt driverproto.AttemptToken, err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	current, started := w.attempt == attempt, w.target.Native != ""
	w.mu.Unlock()
	if !current {
		return
	}
	class := driverproto.FailureProvider
	disposition := driverproto.KeepWorker
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) {
		class, disposition = driverproto.FailureTransport, driverproto.RetireWorker
	}
	if !started {
		w.clearAttempt(attempt)
		w.publish(driverproto.SubmissionRejected{Attempt: attempt, Class: class, Detail: err.Error(), Disposition: disposition})
		return
	}
	// Contradictory testimony remains visible to Runtime, which logs it.
	w.publish(driverproto.SubmissionRejected{Attempt: attempt, Class: class, Detail: err.Error(), Disposition: disposition})
}

func (w *worker) Control(_ context.Context, req driverproto.ControlRequest) {
	if !w.begin(phaseActive) {
		return
	}
	defer w.end()
	w.mu.Lock()
	c, thread, target := w.conn, w.thread, w.target
	w.mu.Unlock()
	if c == nil || target != req.Target || !target.Valid() {
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: req.Target, Verdict: driverproto.ControlTargetGone, Detail: "target is not active", Disposition: driverproto.KeepWorker})
		return
	}
	method := "turn/interrupt"
	params := any(map[string]any{"threadId": thread, "turnId": string(target.Native)})
	if req.Kind == driverproto.ControlSteer {
		if req.Message == nil || req.Message.Text == "" {
			w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlRejected, Detail: "empty steer", Disposition: driverproto.KeepWorker})
			return
		}
		input := buildInput([]driverproto.DriverMessage{*req.Message}, nil, w.cfg.Situation)
		method, params = "turn/steer", map[string]any{"threadId": thread, "expectedTurnId": string(target.Native), "input": input}
	}
	if err := c.rpc.callAsync(method, params, func(_ json.RawMessage, err error) { w.publish(classifyControlOutcome(req, err)) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

func (w *worker) publish(v driverproto.DriverEvent) bool { return w.gate.Publish(v) }

func (w *worker) Retire() {
	w.retireOnce.Do(func() {
		w.gate.Close()
		w.mu.Lock()
		w.phase = phaseRetiring
		c := w.conn
		w.mu.Unlock()
		if c != nil {
			c.Retire()
		}
		go func() {
			w.leases.Wait()
			if c != nil {
				<-c.process.reaped
				<-c.rpc.pumpDone
			}
			w.mu.Lock()
			w.phase = phaseReaped
			w.mu.Unlock()
			close(w.reaped)
		}()
	})
}
func (w *worker) Reaped() <-chan struct{} { return w.reaped }

func (w *worker) clearAttempt(a driverproto.AttemptToken) {
	w.mu.Lock()
	if w.attempt == a {
		w.attempt = 0
		w.target = driverproto.WorkerTurnTarget{}
		if w.phase != phaseRetiring {
			w.phase = phaseReady
		}
	}
	w.mu.Unlock()
}
func (w *worker) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("codex worker thread=%s", w.thread)
}

// dynamicTools projects the host's tool catalog into codex dynamicTools
// (FunctionDynamicToolSpec). Nil when the host has no tools.
func (w *worker) dynamicTools() []map[string]any {
	entries := w.surface.Entries()
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{"type": "function", "name": entry.Wire, "description": entry.Spec.Description, "inputSchema": entry.Spec.Schema})
	}
	return out
}

// dynamicToolCallParams is codex's DynamicToolCallParams (item/tool/call).
type dynamicToolCallParams struct {
	ThreadID  string          `json:"threadId"`
	TurnID    string          `json:"turnId"`
	CallID    string          `json:"callId"`
	Tool      string          `json:"tool"`
	Namespace string          `json:"namespace,omitempty"`
	Arguments json.RawMessage `json:"arguments"`
}

// prepareServerRequest snapshots the active target on the ordered read pump.
// The returned closure may run concurrently but never rereads mutable target
// state, so a late callback cannot be attributed to a newer turn.
func (w *worker) prepareServerRequest(c *connection, method string, params json.RawMessage) func() (any, *rpcError) {
	if method != "item/tool/call" {
		return func() (any, *rpcError) { return handleServerRequest(method, params) }
	}
	var p dynamicToolCallParams
	if err := json.Unmarshal(params, &p); err != nil || p.Tool == "" || p.CallID == "" {
		return func() (any, *rpcError) { return nil, &rpcError{Code: -32602, Message: "invalid item/tool/call params"} }
	}
	w.mu.Lock()
	thread, target, active := w.thread, w.target, w.conn == c && w.phase == phaseActive
	surface := w.surface
	w.mu.Unlock()
	canonical, known := surface.Canonical(p.Tool)
	return func() (any, *rpcError) {
		if !active || p.ThreadID != thread || string(target.Native) != p.TurnID {
			return dynamicToolResult(toolsurface.ErrorText("internal_error", "tool call outside the active turn", "Wait for an active turn and do not reuse a late call"), true), nil
		}
		if !known {
			return nil, &rpcError{Code: -32601, Message: "unknown tool: " + p.Tool}
		}
		args := p.Arguments
		if len(args) == 0 {
			args = json.RawMessage(`{}`)
		}
		var object map[string]any
		if json.Unmarshal(args, &object) != nil || object == nil {
			return nil, &rpcError{Code: -32602, Message: "tool arguments must be an object"}
		}
		ctx, cancel := context.WithTimeout(w.host.GenerationLife(), metatool.MaxSynchronousWait)
		defer cancel()
		res := w.host.Tools().Invoke(ctx, target, driverproto.ToolInvocation{CallID: driverproto.ProviderToolCallID(p.CallID), Name: canonical, Params: args})
		res = surface.MapResult(res)
		return dynamicToolResult(res.Text, res.IsError), nil
	}
}

// dynamicToolResult is codex's DynamicToolCallResponse.
func dynamicToolResult(text string, isError bool) map[string]any {
	return map[string]any{"contentItems": []map[string]any{{"type": "inputText", "text": text}}, "success": !isError}
}

func handleServerRequest(method string, _ json.RawMessage) (any, *rpcError) {
	switch method {
	case "currentTime/read":
		return map[string]any{"currentTimeAt": 0}, nil
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		return map[string]any{"decision": "decline"}, nil
	case "execCommandApproval", "applyPatchApproval":
		return map[string]any{"decision": "denied"}, nil
	case "item/permissions/requestApproval":
		return nil, &rpcError{Code: -32601, Message: "permission escalation unavailable"}
	default:
		return nil, &rpcError{Code: -32601, Message: "method not supported: " + method}
	}
}

func openRejection(err error) driverproto.OpenRejected {
	class := driverproto.FailureProvider
	disposition := driverproto.RetireWorker
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) {
		class = driverproto.FailureTransport
	}
	return driverproto.OpenRejected{Class: class, Detail: err.Error(), Disposition: disposition}
}
func threadIDFrom(raw json.RawMessage) string {
	var v struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(raw, &v)
	return strings.TrimSpace(v.Thread.ID)
}

const resumeSeedPrefix = "atoll-codex-v1:"

func encodeResumeSeed(thread, digest string) []byte {
	return []byte(resumeSeedPrefix + digest + ":" + thread)
}

func decodeResumeSeed(seed []byte, digest string) (string, bool) {
	value := string(seed)
	if !strings.HasPrefix(value, resumeSeedPrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(value, resumeSeedPrefix)
	storedDigest, thread, ok := strings.Cut(rest, ":")
	return thread, ok && thread != "" && storedDigest == digest
}
func deltaNotificationMethods() []string {
	return []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
}

var _ driverproto.Worker = (*worker)(nil)
