package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
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

type worker struct {
	cfg        Config
	host       driverproto.WorkerHost
	mu         sync.Mutex
	retired    bool
	conn       *connection
	thread     string
	attempt    driverproto.AttemptToken
	target     driverproto.WorkerTurnTarget
	final      map[driverproto.WorkerTurnRef]string
	calls      sync.WaitGroup
	reaped     chan struct{}
	retireOnce sync.Once
}

func newWorker(cfg Config, host driverproto.WorkerHost) *worker {
	return &worker{cfg: cfg, host: host, final: map[driverproto.WorkerTurnRef]string{}, reaped: make(chan struct{})}
}
func (w *worker) begin() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retired {
		return false
	}
	w.calls.Add(1)
	return true
}
func (w *worker) reap() {
	w.calls.Wait()
	w.mu.Lock()
	c := w.conn
	w.mu.Unlock()
	if c != nil {
		<-c.process.reaped
		<-c.rpc.pumpDone
	}
	close(w.reaped)
}

func (w *worker) Open(ctx context.Context, req driverproto.OpenRequest) driverproto.OpenResult {
	if !w.begin() {
		return driverproto.OpenReject(driverproto.FailureTransport, "worker retired", driverproto.RetireWorker)
	}
	defer w.calls.Done()
	p, err := w.cfg.processFactory(ctx, w.cfg)
	if err != nil {
		return driverproto.OpenReject(driverproto.FailureTransport, err.Error(), driverproto.RetireWorker)
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
			w.host.Events().Publish(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		}
	}
	w.mu.Lock()
	if w.retired {
		w.mu.Unlock()
		c.Retire()
		return driverproto.OpenReject(driverproto.FailureTransport, "worker retired", driverproto.RetireWorker)
	}
	w.conn = c
	w.mu.Unlock()
	c.rpc.start()
	params := map[string]any{"clientInfo": map[string]any{"name": "atoll", "title": "Atoll Codex Agent", "version": "1"}, "capabilities": map[string]any{"experimentalApi": true, "optOutNotificationMethods": deltaNotificationMethods()}}
	rawInit, err := c.rpc.call(ctx, "initialize", params, initializeTimeout)
	if err != nil {
		c.Retire()
		return classifyOpen(err)
	}
	var initResult struct {
		UserAgent string `json:"userAgent"`
	}
	_ = json.Unmarshal(rawInit, &initResult)
	if err = c.rpc.notify("initialized", map[string]any{}); err != nil {
		c.Retire()
		return driverproto.OpenUncertain(driverproto.FailureTransport, err.Error())
	}
	thread, res := w.establishSession(ctx, c, string(req.ResumeSeed))
	if res.Validate() != nil || res.Verdict() != driverproto.OpenReady {
		c.Retire()
		return res
	}
	w.mu.Lock()
	if w.retired {
		w.mu.Unlock()
		c.Retire()
		return driverproto.OpenReject(driverproto.FailureTransport, "worker retired", driverproto.RetireWorker)
	}
	w.thread = thread
	w.mu.Unlock()
	w.host.Events().Publish(driverproto.SeedUpdated{Value: []byte(thread)})
	return driverproto.Ready()
}

func (w *worker) Start(ctx context.Context, req driverproto.StartRequest) driverproto.StartResult {
	if !w.begin() {
		return driverproto.StartReject(driverproto.FailureTransport, "worker retired", driverproto.RetireWorker)
	}
	defer w.calls.Done()
	input, err := buildInput(req.Messages, req.Background)
	if err != nil {
		return driverproto.StartReject(driverproto.FailureInvalidInput, err.Error(), driverproto.KeepWorker)
	}
	w.mu.Lock()
	c, thread := w.conn, w.thread
	if c == nil || w.attempt != 0 {
		w.mu.Unlock()
		return driverproto.StartReject(driverproto.FailureProvider, "worker not ready or turn active", driverproto.KeepWorker)
	}
	w.attempt = req.Attempt
	w.target = driverproto.WorkerTurnTarget{Attempt: req.Attempt}
	w.mu.Unlock()
	_, err = c.rpc.call(ctx, "turn/start", map[string]any{"threadId": thread, "input": input}, rpcTimeout)
	if err == nil {
		return driverproto.StartAccept(driverproto.KeepWorker)
	}
	var rpcErr *rpcError
	if errors.As(err, &rpcErr) {
		w.mu.Lock()
		started := w.target.Attempt == req.Attempt && w.target.Native != ""
		w.mu.Unlock()
		if started {
			return driverproto.StartUncertain(driverproto.FailureProvider, err.Error())
		}
		w.clearAttempt(req.Attempt)
		return driverproto.StartReject(driverproto.FailureProvider, err.Error(), driverproto.KeepWorker)
	}
	return driverproto.StartUncertain(driverproto.FailureTransport, err.Error())
}

func (w *worker) Control(ctx context.Context, req driverproto.ControlRequest) driverproto.ControlResult {
	if !w.begin() {
		return driverproto.ControlReject(driverproto.FailureTransport, "worker retired", driverproto.RetireWorker)
	}
	defer w.calls.Done()
	w.mu.Lock()
	c, thread, target := w.conn, w.thread, w.target
	w.mu.Unlock()
	if c == nil || target != req.Target || !target.Valid() {
		return driverproto.TargetGone("target is not active", driverproto.KeepWorker)
	}
	method := "turn/interrupt"
	params := map[string]any{"threadId": thread, "turnId": string(target.Native)}
	if req.Kind == driverproto.ControlSteer {
		if req.Message == nil || req.Message.Text == "" {
			return driverproto.ControlReject(driverproto.FailureInvalidInput, "empty steer", driverproto.KeepWorker)
		}
		input, err := buildInput([]driverproto.DriverMessage{*req.Message}, nil)
		if err != nil {
			return driverproto.ControlReject(driverproto.FailureInvalidInput, err.Error(), driverproto.KeepWorker)
		}
		method = "turn/steer"
		params = map[string]any{"threadId": thread, "expectedTurnId": string(target.Native), "input": input}
	}
	_, err := c.rpc.call(ctx, method, params, rpcTimeout)
	return classifyControl(err, req.Kind)
}

func (w *worker) Retire() {
	w.retireOnce.Do(func() {
		w.mu.Lock()
		w.retired = true
		c := w.conn
		w.mu.Unlock()
		if c != nil {
			c.Retire()
		}
		go w.reap()
	})
}
func (w *worker) Reaped() <-chan struct{} { return w.reaped }
func (w *worker) clearAttempt(a driverproto.AttemptToken) {
	w.mu.Lock()
	if w.attempt == a {
		w.attempt = 0
		w.target = driverproto.WorkerTurnTarget{}
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
func classifyOpen(err error) driverproto.OpenResult {
	if isInvalidResumeError(err) {
		return driverproto.ResumeInvalid(err.Error())
	}
	var rpcErr *rpcError
	if errors.As(err, &rpcErr) {
		return driverproto.OpenReject(driverproto.FailureProvider, err.Error(), driverproto.RetireWorker)
	}
	return driverproto.OpenUncertain(driverproto.FailureTransport, err.Error())
}

func deltaNotificationMethods() []string {
	return []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
}

var _ driverproto.Worker = (*worker)(nil)
