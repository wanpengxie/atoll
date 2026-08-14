package script

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type workerPhase uint8

const (
	phaseConstructed workerPhase = iota
	phaseOpening
	phaseReady
	phaseActive
	phaseRetiring
	phaseReaped
)

type worker struct {
	toolID    string
	toolType  string
	host      driverproto.WorkerHost
	mu        sync.Mutex
	phase     workerPhase
	producing bool
	target    driverproto.WorkerTurnTarget
	callbacks int
	calls     sync.WaitGroup
	reaped    chan struct{}
	once      sync.Once
}

func newWorker(toolID, toolType string, h driverproto.WorkerHost) *worker {
	return &worker{toolID: toolID, toolType: toolType, host: h, producing: true, reaped: make(chan struct{})}
}

func (w *worker) Open(context.Context, driverproto.OpenRequest) {
	w.mu.Lock()
	if w.phase != phaseConstructed {
		w.mu.Unlock()
		return
	}
	w.phase = phaseOpening
	w.calls.Add(1)
	w.mu.Unlock()
	defer w.calls.Done()
	w.mu.Lock()
	if w.phase == phaseOpening {
		w.phase = phaseReady
	}
	w.mu.Unlock()
	w.publish(driverproto.WorkerReady{})
}

func (w *worker) Start(_ context.Context, req driverproto.StartRequest) {
	w.mu.Lock()
	if w.phase != phaseReady {
		w.mu.Unlock()
		return
	}
	w.calls.Add(1)
	if len(req.Messages) == 0 {
		w.mu.Unlock()
		w.publish(driverproto.SubmissionRejected{Attempt: req.Attempt, Class: driverproto.FailureInvalidInput, Detail: "empty input", Disposition: driverproto.KeepWorker})
		w.calls.Done()
		return
	}
	target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: driverproto.WorkerTurnRef(fmt.Sprintf("script-%d", req.Attempt))}
	w.phase, w.target = phaseActive, target
	w.mu.Unlock()
	m := req.Messages[len(req.Messages)-1]
	go func() {
		defer w.calls.Done()
		if !w.publish(driverproto.TurnStarted{Target: target}) {
			return
		}
		text, detail := w.execute(req.Life, target, m)
		status := driverproto.TurnOK
		if detail != "" {
			status = driverproto.TurnFailed
		}
		w.mu.Lock()
		if w.phase != phaseActive || w.target != target || w.callbacks != 0 {
			w.mu.Unlock()
			return
		}
		w.phase, w.target = phaseReady, driverproto.WorkerTurnTarget{}
		w.mu.Unlock()
		w.publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: text, ErrorDetail: detail})
	}()
}

func (w *worker) Control(_ context.Context, req driverproto.ControlRequest) {
	w.mu.Lock()
	if w.phase != phaseActive || w.target != req.Target {
		w.mu.Unlock()
		return
	}
	w.calls.Add(1)
	w.mu.Unlock()
	defer w.calls.Done()
	w.publish(driverproto.ControlOutcome{Action: req.Action, Target: req.Target, Verdict: driverproto.ControlRejected, Detail: "script controls are unsupported", Disposition: driverproto.KeepWorker})
}

func (w *worker) publish(event driverproto.DriverEvent) bool {
	w.mu.Lock()
	allowed := w.producing && w.phase != phaseRetiring && w.phase != phaseReaped
	w.mu.Unlock()
	if !allowed {
		return false
	}
	ok := w.host.Events().Publish(event)
	if !ok {
		w.mu.Lock()
		w.producing = false
		w.mu.Unlock()
	}
	return ok
}

func (w *worker) Retire() {
	w.once.Do(func() {
		w.mu.Lock()
		w.phase, w.producing = phaseRetiring, false
		w.mu.Unlock()
		go func() { w.calls.Wait(); w.mu.Lock(); w.phase = phaseReaped; w.mu.Unlock(); close(w.reaped) }()
	})
}
func (w *worker) Reaped() <-chan struct{} { return w.reaped }

func (w *worker) beginCallback(target driverproto.WorkerTurnTarget) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.phase != phaseActive || w.target != target {
		return false
	}
	w.callbacks++
	return true
}
func (w *worker) endCallback() { w.mu.Lock(); w.callbacks--; w.mu.Unlock() }
func (w *worker) tool(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ToolInvocation) driverproto.ToolResult {
	if !w.beginCallback(target) {
		return driverproto.ToolResult{Text: "turn is not active", IsError: true}
	}
	defer w.endCallback()
	return w.host.Tools().Invoke(ctx, target, in)
}
func (w *worker) resource(ctx context.Context, target driverproto.WorkerTurnTarget, in driverproto.ResourceInvocation) driverproto.ResourceResult {
	if !w.beginCallback(target) {
		return driverproto.ResourceResult{Error: "turn is not active"}
	}
	defer w.endCallback()
	return w.host.Resources().Invoke(ctx, target, in)
}

func (w *worker) execute(ctx context.Context, target driverproto.WorkerTurnTarget, m driverproto.DriverMessage) (string, string) {
	switch m.Type {
	case TypeChat:
		return w.chat(ctx, target, m)
	case TypeVerify:
		return w.verify(ctx, target, m)
	default:
		return "", "type_unsupported: script does not handle " + m.Type
	}
}
func (w *worker) chat(ctx context.Context, target driverproto.WorkerTurnTarget, m driverproto.DriverMessage) (string, string) {
	var payload map[string]any
	if json.Unmarshal(m.Payload, &payload) != nil || payload == nil {
		return "", "bad_payload: loop.chat payload must be a JSON object"
	}
	params, _ := json.Marshal(map[string]any{"actor_id": w.toolID, "type": w.toolType, "payload": payload, "wait": true})
	tool := w.tool(ctx, target, driverproto.ToolInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("call-%d", target.Attempt)), Name: "call_actor", Params: params})
	if tool.IsError {
		return "", "tool_call_failed: " + tool.Text
	}
	var echoed map[string]any
	if json.Unmarshal([]byte(tool.Text), &echoed) != nil {
		return "", "tool_call_failed: invalid tool result"
	}
	rid := "file:loop/" + m.SourceID
	res := w.resource(ctx, target, driverproto.ResourceInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("write-%d", target.Attempt)), Operation: "write_file", ResourceID: rid, Payload: m.Payload})
	if res.Error != "" {
		return "", "resource_failed: " + res.Error
	}
	raw, _ := json.Marshal(map[string]any{"ok": true, "echoed": echoed, "resource_id": rid})
	return string(raw), ""
}
func (w *worker) verify(ctx context.Context, target driverproto.WorkerTurnTarget, m driverproto.DriverMessage) (string, string) {
	var p struct {
		ResourceID string `json:"resource_id"`
	}
	if json.Unmarshal(m.Payload, &p) != nil || strings.TrimSpace(p.ResourceID) == "" {
		return "", "bad_payload: loop.verify requires resource_id"
	}
	out := w.resource(ctx, target, driverproto.ResourceInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("read-%d", target.Attempt)), Operation: "read_file", ResourceID: p.ResourceID})
	if out.Error != "" {
		return "", "resource_failed: " + out.Error
	}
	var value map[string]any
	if json.Unmarshal(out.Payload, &value) != nil {
		return "", "resource_failed: invalid read result"
	}
	value["exists"] = true
	raw, _ := json.Marshal(value)
	return string(raw), ""
}

var _ driverproto.Worker = (*worker)(nil)
