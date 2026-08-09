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
	"github.com/wanpengxie/atoll/drivers/agents/provider/codex/internal/emit"
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
	cfg        Config
	gate       *emit.Gate
	mu         sync.Mutex
	phase      workerPhase
	conn       *connection
	thread     string
	attempt    driverproto.AttemptToken
	target     driverproto.WorkerTurnTarget
	final      map[driverproto.WorkerTurnRef]string
	leases     sync.WaitGroup
	reaped     chan struct{}
	retireOnce sync.Once
}

func newWorker(cfg Config, host driverproto.WorkerHost) *worker {
	return &worker{cfg: cfg, gate: emit.New(host.Events()), phase: phaseConstructed, final: map[driverproto.WorkerTurnRef]string{}, reaped: make(chan struct{})}
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
	c.rpc.onRequest = handleServerRequest
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
	method, params := "thread/start", any(map[string]any{"approvalPolicy": "never", "sandbox": "danger-full-access", "cwd": w.cfg.WorkspaceDir})
	if len(seed) > 0 {
		method, params = "thread/resume", map[string]any{"threadId": string(seed), "excludeTurns": true}
	}
	if err := c.rpc.callAsync(method, params, func(raw json.RawMessage, err error) { w.afterSession(c, len(seed) > 0, raw, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
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
	w.mu.Lock()
	if w.phase != phaseOpening || w.conn != c {
		w.mu.Unlock()
		return
	}
	w.thread, w.phase = id, phaseReady
	w.mu.Unlock()
	if !w.publish(driverproto.SeedUpdated{Value: []byte(id)}) {
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
	input := buildInput(req.Messages, req.Background)
	w.mu.Lock()
	if w.phase != phaseReady || w.conn == nil {
		w.mu.Unlock()
		return
	}
	c, thread := w.conn, w.thread
	w.phase, w.attempt = phaseStarting, req.Attempt
	w.target = driverproto.WorkerTurnTarget{Attempt: req.Attempt}
	w.mu.Unlock()
	if err := c.rpc.callAsync("turn/start", map[string]any{"threadId": thread, "input": input}, func(_ json.RawMessage, err error) { w.afterStartResponse(req.Attempt, err) }); err != nil {
		w.publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
	}
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
		input := buildInput([]driverproto.DriverMessage{*req.Message}, nil)
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
func deltaNotificationMethods() []string {
	return []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
}

var _ driverproto.Worker = (*worker)(nil)
