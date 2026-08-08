package script

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type worker struct {
	toolID  string
	host    driverproto.WorkerHost
	mu      sync.Mutex
	retired bool
	calls   sync.WaitGroup
	reaped  chan struct{}
	once    sync.Once
}

func newWorker(toolID string, h driverproto.WorkerHost) *worker {
	return &worker{toolID: toolID, host: h, reaped: make(chan struct{})}
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
func (w *worker) Open(context.Context, driverproto.OpenRequest) driverproto.OpenResult {
	if !w.begin() {
		return driverproto.OpenReject(driverproto.FailureTransport, "retired", driverproto.RetireWorker)
	}
	defer w.calls.Done()
	return driverproto.Ready()
}
func (w *worker) Start(_ context.Context, req driverproto.StartRequest) driverproto.StartResult {
	if !w.begin() {
		return driverproto.StartReject(driverproto.FailureTransport, "retired", driverproto.RetireWorker)
	}
	if len(req.Messages) == 0 {
		w.calls.Done()
		return driverproto.StartReject(driverproto.FailureInvalidInput, "empty input", driverproto.KeepWorker)
	}
	target := driverproto.WorkerTurnTarget{Attempt: req.Attempt, Native: driverproto.WorkerTurnRef(fmt.Sprintf("script-%d", req.Attempt))}
	if !w.host.Events().Publish(driverproto.TurnStarted{Target: target}) {
		w.calls.Done()
		return driverproto.StartReject(driverproto.FailureTransport, "event sink closed", driverproto.RetireWorker)
	}
	m := req.Messages[len(req.Messages)-1]
	go func() {
		defer w.calls.Done()
		text, detail := w.execute(req.Life, target, m)
		status := driverproto.TurnOK
		if detail != "" {
			status = driverproto.TurnFailed
		}
		w.host.Events().Publish(driverproto.TurnEnded{Target: target, Status: status, FinalText: text, ErrorDetail: detail})
	}()
	return driverproto.StartAccept(driverproto.KeepWorker)
}
func (w *worker) Control(context.Context, driverproto.ControlRequest) driverproto.ControlResult {
	return driverproto.ControlReject(driverproto.FailureProvider, "script controls are unsupported", driverproto.KeepWorker)
}
func (w *worker) Retire() {
	w.once.Do(func() { w.mu.Lock(); w.retired = true; w.mu.Unlock(); go func() { w.calls.Wait(); close(w.reaped) }() })
}
func (w *worker) Reaped() <-chan struct{} { return w.reaped }
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
	params, _ := json.Marshal(map[string]any{"actor_id": w.toolID, "type": toolSayType, "payload": payload, "wait": true})
	tool := w.host.Tools().Invoke(ctx, target, driverproto.ToolInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("call-%d", target.Attempt)), Name: "call_actor", Params: params})
	if tool.IsError {
		return "", "tool_call_failed: " + tool.Text
	}
	var echoed map[string]any
	if json.Unmarshal([]byte(tool.Text), &echoed) != nil {
		return "", "tool_call_failed: invalid tool result"
	}
	rid := "file:loop/" + m.SourceID
	res := w.host.Resources().Invoke(ctx, target, driverproto.ResourceInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("write-%d", target.Attempt)), Operation: "write_file", ResourceID: rid, Payload: m.Payload})
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
	out := w.host.Resources().Invoke(ctx, target, driverproto.ResourceInvocation{CallID: driverproto.ProviderToolCallID(fmt.Sprintf("read-%d", target.Attempt)), Operation: "read_file", ResourceID: p.ResourceID})
	if out.Error != "" {
		return "", "resource_failed: " + out.Error
	}
	var v map[string]any
	if json.Unmarshal(out.Payload, &v) != nil {
		return "", "resource_failed: invalid read result"
	}
	v["exists"] = true
	raw, _ := json.Marshal(v)
	return string(raw), ""
}

var _ driverproto.Worker = (*worker)(nil)
