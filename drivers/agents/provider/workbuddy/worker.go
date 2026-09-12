package workbuddy

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
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/mcpcodec"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/mcphttp"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

type connection struct {
	process *childProcess
	rpc     *rpcClient
	mcp     *mcphttp.Server
	retired atomic.Bool
}

func (c *connection) Retire() {
	if c != nil && !c.retired.Swap(true) {
		if c.mcp != nil {
			c.mcp.Close()
		}
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
	cfg              Config
	host             driverproto.WorkerHost
	gate             *emit.Gate
	mu               sync.Mutex
	phase            workerPhase
	conn             *connection
	session          string
	attempt          driverproto.AttemptToken
	target           driverproto.WorkerTurnTarget
	kind             driverproto.TurnKind
	options          driverproto.TurnOptions
	usage            driverproto.TurnUsage
	final, thinking  strings.Builder
	availableModels  map[string]bool
	availableEfforts map[string]bool
	surface          toolsurface.Surface
	leases           sync.WaitGroup
	reaped           chan struct{}
	retireOnce       sync.Once
}

func newWorker(cfg Config, host driverproto.WorkerHost) *worker {
	return &worker{cfg: cfg, host: host, gate: emit.New(host.Events()), phase: phaseConstructed, reaped: make(chan struct{})}
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
func (w *worker) end()                                   { w.leases.Done() }
func (w *worker) publish(v driverproto.DriverEvent) bool { return w.gate.Publish(v) }

func (w *worker) Open(ctx context.Context, req driverproto.OpenRequest) {
	if !w.begin(phaseConstructed) {
		return
	}
	defer w.end()
	w.mu.Lock()
	w.phase = phaseOpening
	w.options = req.Options
	w.mu.Unlock()
	if w.host == nil || w.host.Tools() == nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: "tool host unavailable", Disposition: driverproto.RetireWorker})
		return
	}
	surface, err := toolsurface.Assemble(w.host.Tools().Catalog(), toolsurface.Claude, w.cfg.Situation)
	if err != nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	w.surface = surface
	resume, resumeOK := decodeResumeSeed(req.ResumeSeed, surface.Digest())
	if len(req.ResumeSeed) > 0 && !resumeOK {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureResumeInvalid, Detail: "workbuddy resume seed does not match the current tool surface", Disposition: driverproto.RetireWorker})
		return
	}
	var c *connection
	mcp, err := mcphttp.Start(w.host.GenerationLife(), surface, func() mcpcodec.InvokeFunc {
		w.mu.Lock()
		target, active := w.target, w.phase == phaseActive
		w.mu.Unlock()
		if !active || !target.Valid() {
			return nil
		}
		return func(callCtx context.Context, inv driverproto.ToolInvocation) driverproto.ToolResult {
			return w.host.Tools().Invoke(callCtx, target, inv)
		}
	}, w.cfg.Logger)
	if err != nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	p, err := w.cfg.processFactory(ctx, w.cfg)
	if err != nil {
		mcp.Close()
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	c = &connection{process: p, mcp: mcp}
	c.rpc = newRPC(p)
	c.rpc.onNotification = func(method string, params json.RawMessage) {
		if !c.retired.Load() {
			w.notification(c, method, params)
		}
	}
	c.rpc.onRequest = handleClientRequest
	c.rpc.onClose = func(err error) {
		if !c.retired.Load() {
			cause := driverproto.WorkerTransportEnded
			if !p.stopped.Load() {
				select {
				case exitErr := <-p.exit:
					if exitErr != nil {
						cause = driverproto.WorkerCrash
						err = exitErr
					}
				default:
				}
			}
			w.publish(driverproto.WorkerEnded{Cause: cause, Detail: err.Error()})
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
	params := map[string]any{"protocolVersion": 1, "clientInfo": map[string]any{"name": "atoll", "version": "1"}, "clientCapabilities": map[string]any{}}
	if err := c.rpc.callAsync("initialize", params, func(raw json.RawMessage, err error) { w.afterInitialize(c, resume, resumeOK, raw, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

func (w *worker) afterInitialize(c *connection, resume string, resumeOK bool, _ json.RawMessage, err error) {
	if !w.isOpening(c) {
		return
	}
	if err != nil {
		w.publish(openRejection(err))
		return
	}
	params := map[string]any{"cwd": w.cfg.WorkspaceDir, "mcpServers": c.mcp.ACPConfig()}
	method := "session/new"
	if resumeOK {
		method = "session/load"
		params["sessionId"] = resume
	}
	if err := c.rpc.callAsync(method, params, func(raw json.RawMessage, err error) { w.afterSession(c, resume, resumeOK, raw, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
}

type sessionResponse struct {
	SessionID string `json:"sessionId"`
	Models    *struct {
		Available []struct {
			ID   string `json:"modelId"`
			Name string `json:"name"`
		} `json:"availableModels"`
		Current string `json:"currentModelId"`
	} `json:"models"`
	ConfigOptions []struct {
		ID      string `json:"id"`
		Current string `json:"currentValue"`
		Options []struct {
			Value string `json:"value"`
		} `json:"options"`
	} `json:"configOptions"`
}

func (w *worker) afterSession(c *connection, resume string, resumed bool, raw json.RawMessage, err error) {
	if !w.isOpening(c) {
		return
	}
	if err != nil {
		class := driverproto.FailureProvider
		if resumed {
			class = driverproto.FailureResumeInvalid
		}
		w.publish(driverproto.OpenRejected{Class: class, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	var s sessionResponse
	if json.Unmarshal(raw, &s) == nil && resumed && s.SessionID == "" {
		s.SessionID = resume // ACP load responses do not repeat the requested id.
	}
	if strings.TrimSpace(s.SessionID) == "" {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: "session response missing session id", Disposition: driverproto.RetireWorker})
		return
	}
	models, efforts := catalogs(s)
	w.mu.Lock()
	w.session = s.SessionID
	w.availableModels, w.availableEfforts = models, efforts
	if w.options.Model == "" && s.Models != nil {
		w.options.Model = s.Models.Current
	}
	if w.options.Effort == "" {
		w.options.Effort = configCurrent(s, "thought_level")
	}
	w.mu.Unlock()
	if err := validateSelection(w.options, models, efforts); err != nil {
		w.publish(driverproto.OpenRejected{Class: driverproto.FailureProvider, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	w.applySelection(c, s.SessionID, w.options, driverproto.TurnOptions{}, func(err error) {
		if !w.isOpening(c) {
			return
		}
		if err != nil {
			w.publish(openRejection(err))
			return
		}
		w.mu.Lock()
		w.phase = phaseReady
		w.mu.Unlock()
		if !w.publish(driverproto.SeedUpdated{Value: encodeResumeSeed(s.SessionID, w.surface.Digest())}) {
			return
		}
		w.publish(driverproto.WorkerReady{})
	})
}

func catalogs(s sessionResponse) (map[string]bool, map[string]bool) {
	m := map[string]bool{}
	e := map[string]bool{}
	if s.Models != nil {
		for _, x := range s.Models.Available {
			m[x.ID] = true
		}
	}
	for _, c := range s.ConfigOptions {
		if c.ID == "thought_level" {
			for _, x := range c.Options {
				e[x.Value] = true
			}
		}
	}
	return m, e
}
func configCurrent(s sessionResponse, id string) string {
	for _, c := range s.ConfigOptions {
		if c.ID == id {
			return c.Current
		}
	}
	return ""
}
func validateSelection(o driverproto.TurnOptions, models, efforts map[string]bool) error {
	if o.Model != "" && !models[o.Model] {
		return localProviderError{fmt.Sprintf("WorkBuddy 当前订阅不可用模型 %q", o.Model)}
	}
	if o.Effort != "" && !efforts[o.Effort] {
		return localProviderError{fmt.Sprintf("WorkBuddy 不支持 thought_level %q", o.Effort)}
	}
	return nil
}

func (w *worker) applySelection(c *connection, session string, next, previous driverproto.TurnOptions, done func(error)) {
	afterModel := func(err error) {
		if err != nil {
			done(err)
			return
		}
		if next.Effort == "" || next.Effort == previous.Effort {
			done(nil)
			return
		}
		params := map[string]any{"sessionId": session, "configId": "thought_level", "value": next.Effort}
		if err := c.rpc.callAsync("session/set_config_option", params, func(_ json.RawMessage, effortErr error) {
			if effortErr != nil && next.Model != "" && next.Model != previous.Model && previous.Model != "" {
				_ = c.rpc.callAsync("session/set_model", map[string]any{"sessionId": session, "modelId": previous.Model}, func(_ json.RawMessage, _ error) { done(effortErr) })
				return
			}
			done(effortErr)
		}); err != nil {
			done(err)
		}
	}
	if next.Model == "" || next.Model == previous.Model {
		afterModel(nil)
		return
	}
	if err := c.rpc.callAsync("session/set_model", map[string]any{"sessionId": session, "modelId": next.Model}, func(_ json.RawMessage, err error) { afterModel(err) }); err != nil {
		done(err)
	}
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
	c, session := w.conn, w.session
	w.phase, w.attempt, w.kind = phaseStarting, req.Attempt, req.Kind
	w.target = driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: driverproto.WorkerTurnRef(fmt.Sprintf("workbuddy-%d", req.Attempt))}
	target := w.target
	w.final.Reset()
	w.thinking.Reset()
	previous := w.options
	next := req.Options
	if next.Model == "" {
		next.Model = previous.Model
	}
	if next.Effort == "" {
		next.Effort = previous.Effort
	}
	w.mu.Unlock()
	if req.Kind == driverproto.TurnSelect {
		if err := validateSelection(next, w.availableModels, w.availableEfforts); err != nil {
			w.reject(req.Attempt, err)
			return
		}
		w.applySelection(c, session, next, previous, func(err error) {
			if err != nil {
				w.reject(req.Attempt, err)
				return
			}
			w.mu.Lock()
			if w.attempt != req.Attempt {
				w.mu.Unlock()
				return
			}
			w.options = next
			w.phase = phaseActive
			usage := w.currentUsageLocked()
			w.mu.Unlock()
			w.publish(driverproto.TurnStarted{Target: target})
			w.finish(req.Attempt, driverproto.TurnOK, "", usage)
		})
		return
	}
	if req.Kind == driverproto.TurnNew {
		w.newSession(c, req, target, next)
		return
	}
	content := buildContent(req.Messages, req.Background, w.cfg.Situation)
	if req.Kind == driverproto.TurnCompact {
		content = []map[string]any{{"type": "text", "text": "/compact"}}
	}
	if len(content) == 0 {
		w.reject(req.Attempt, invalidInputError{"empty input"})
		return
	}
	w.mu.Lock()
	if w.attempt == req.Attempt {
		w.phase = phaseActive
	}
	w.mu.Unlock()
	w.publish(driverproto.TurnStarted{Target: target})
	if err := c.rpc.callAsync("session/prompt", map[string]any{"sessionId": session, "prompt": content}, func(raw json.RawMessage, err error) { w.afterPrompt(req.Attempt, target, raw, err) }); err != nil {
		w.reject(req.Attempt, err)
	}
}

func (w *worker) newSession(c *connection, req driverproto.StartRequest, target driverproto.WorkerTurnTarget, next driverproto.TurnOptions) {
	params := map[string]any{"cwd": w.cfg.WorkspaceDir, "mcpServers": c.mcp.ACPConfig()}
	if err := c.rpc.callAsync("session/new", params, func(raw json.RawMessage, err error) {
		if err != nil {
			w.reject(req.Attempt, err)
			return
		}
		var s sessionResponse
		if json.Unmarshal(raw, &s) != nil || s.SessionID == "" {
			w.reject(req.Attempt, localProviderError{"new session response missing session id"})
			return
		}
		models, efforts := catalogs(s)
		if err := validateSelection(next, models, efforts); err != nil {
			w.reject(req.Attempt, err)
			return
		}
		w.applySelection(c, s.SessionID, next, driverproto.TurnOptions{}, func(err error) {
			if err != nil {
				w.reject(req.Attempt, err)
				return
			}
			w.mu.Lock()
			if w.attempt != req.Attempt {
				w.mu.Unlock()
				return
			}
			w.session = s.SessionID
			w.availableModels, w.availableEfforts = models, efforts
			w.options = next
			w.usage.ContextTokens = 0
			w.phase = phaseActive
			usage := w.currentUsageLocked()
			w.mu.Unlock()
			w.publish(driverproto.TurnStarted{Target: target})
			if !w.publish(driverproto.SeedUpdated{Value: encodeResumeSeed(s.SessionID, w.surface.Digest())}) {
				return
			}
			w.finish(req.Attempt, driverproto.TurnOK, "", usage)
		})
	}); err != nil {
		w.reject(req.Attempt, err)
	}
}

func (w *worker) afterPrompt(attempt driverproto.AttemptToken, target driverproto.WorkerTurnTarget, raw json.RawMessage, err error) {
	w.mu.Lock()
	if w.attempt != attempt || w.target != target {
		w.mu.Unlock()
		return
	}
	thinking := w.thinking.String()
	usage := w.currentUsageLocked()
	w.mu.Unlock()
	if thinking != "" {
		w.publish(driverproto.ProgressNote{Target: target, Kind: driverproto.NoteThinking, Text: bounded(thinking)})
	}
	status, detail := driverproto.TurnOK, ""
	if err != nil {
		status = driverproto.TurnFailed
		if strings.Contains(strings.ToLower(err.Error()), "cancel") || strings.Contains(strings.ToLower(err.Error()), "interrupt") {
			status = driverproto.TurnInterrupted
		}
		detail = err.Error()
	} else {
		var r struct {
			StopReason string `json:"stopReason"`
		}
		_ = json.Unmarshal(raw, &r)
		if r.StopReason == "cancelled" {
			status = driverproto.TurnInterrupted
		}
	}
	w.finish(attempt, status, detail, usage)
}
func (w *worker) finish(attempt driverproto.AttemptToken, status driverproto.TurnEndStatus, detail string, usage driverproto.TurnUsage) {
	w.mu.Lock()
	if w.attempt != attempt {
		w.mu.Unlock()
		return
	}
	target := w.target
	final := w.final.String()
	w.attempt = 0
	w.target = driverproto.WorkerTurnTarget{}
	w.phase = phaseReady
	w.mu.Unlock()
	w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: final, ErrorDetail: redactNative(detail), Usage: usage})
}
func (w *worker) reject(attempt driverproto.AttemptToken, err error) {
	w.mu.Lock()
	current := w.attempt == attempt
	if current {
		w.attempt = 0
		w.target = driverproto.WorkerTurnTarget{}
		w.phase = phaseReady
	}
	w.mu.Unlock()
	if current {
		class := driverproto.FailureProvider
		var rpcErr *rpcError
		var invalid invalidInputError
		var local localProviderError
		if errors.As(err, &invalid) {
			class = driverproto.FailureInvalidInput
		} else if errors.As(err, &local) || errors.As(err, &rpcErr) {
			class = driverproto.FailureProvider
		} else {
			class = driverproto.FailureTransport
		}
		w.publish(driverproto.SubmissionRejected{Attempt: attempt, Class: class, Detail: err.Error(), Disposition: driverproto.KeepWorker})
	}
}

type invalidInputError struct{ detail string }

func (e invalidInputError) Error() string { return e.detail }

type localProviderError struct{ detail string }

func (e localProviderError) Error() string { return e.detail }
func (w *worker) currentUsageLocked() driverproto.TurnUsage {
	u := w.usage
	u.Model = w.options.Model
	u.Effort = w.options.Effort
	return u
}

func (w *worker) Control(_ context.Context, req driverproto.ControlRequest) {
	if !w.begin(phaseActive) {
		return
	}
	defer w.end()
	w.mu.Lock()
	c, session, target := w.conn, w.session, w.target
	w.mu.Unlock()
	if c == nil || target != req.Target || !target.Valid() {
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: req.Target, Verdict: driverproto.ControlTargetGone, Detail: "target is not active", Disposition: driverproto.KeepWorker})
		return
	}
	if req.Kind == driverproto.ControlInterrupt {
		if err := c.rpc.notify("session/cancel", map[string]any{"sessionId": session}); err != nil {
			w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlRejected, Detail: err.Error(), Disposition: driverproto.RetireWorker})
			return
		}
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlAccepted, Disposition: driverproto.KeepWorker})
		return
	}
	if req.Message == nil || strings.TrimSpace(req.Message.Text) == "" {
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlRejected, Detail: "empty steer", Disposition: driverproto.KeepWorker})
		return
	}
	content := buildContent([]driverproto.DriverMessage{*req.Message}, nil, w.cfg.Situation)
	params := map[string]any{"sessionId": session, "contentBlocks": content}
	if err := c.rpc.callAsync("session/steer", params, func(raw json.RawMessage, err error) {
		verdict, detail := driverproto.ControlAccepted, ""
		if err != nil {
			verdict, detail = driverproto.ControlRejected, err.Error()
		} else {
			var r struct {
				Steered bool   `json:"steered"`
				Reason  string `json:"reason"`
			}
			_ = json.Unmarshal(raw, &r)
			if !r.Steered {
				verdict, detail = driverproto.ControlNotSteerable, r.Reason
			}
		}
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: verdict, Detail: detail, Disposition: driverproto.KeepWorker})
	}); err != nil {
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlRejected, Detail: err.Error(), Disposition: driverproto.RetireWorker})
	}
}

func (w *worker) isOpening(c *connection) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.phase == phaseOpening && w.conn == c
}
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
func (w *worker) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("workbuddy worker session=%s", w.session)
}

func handleClientRequest(method string, _ json.RawMessage) (any, *rpcError) {
	switch method {
	case "session/request_permission":
		return map[string]any{"outcome": map[string]any{"outcome": "cancelled"}}, nil
	case "_codebuddy.ai/question":
		return map[string]any{"outcome": "cancelled"}, nil
	default:
		return nil, &rpcError{Code: -32601, Message: "method not supported: " + method}
	}
}
func openRejection(err error) driverproto.OpenRejected {
	class := driverproto.FailureProvider
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) {
		class = driverproto.FailureTransport
	}
	return driverproto.OpenRejected{Class: class, Detail: err.Error(), Disposition: driverproto.RetireWorker}
}

const resumeSeedPrefix = "atoll-workbuddy-v1:"

func encodeResumeSeed(session, digest string) []byte {
	return []byte(resumeSeedPrefix + digest + ":" + session)
}
func decodeResumeSeed(seed []byte, digest string) (string, bool) {
	v := string(seed)
	if !strings.HasPrefix(v, resumeSeedPrefix) {
		return "", false
	}
	stored, session, ok := strings.Cut(strings.TrimPrefix(v, resumeSeedPrefix), ":")
	return session, ok && session != "" && stored == digest
}
func bounded(s string) string {
	r := []rune(redactNative(strings.TrimSpace(s)))
	if len(r) <= 4096 {
		return string(r)
	}
	return string(r[:4084]) + "…[truncated]"
}

var _ driverproto.Worker = (*worker)(nil)
