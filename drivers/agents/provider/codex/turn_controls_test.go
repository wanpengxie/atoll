package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/metatool"
)

type codexEventSink struct {
	mu     sync.Mutex
	events []driverproto.DriverEvent
	wake   chan driverproto.DriverEvent
}

func newCodexEventSink() *codexEventSink {
	return &codexEventSink{wake: make(chan driverproto.DriverEvent, 128)}
}
func (s *codexEventSink) Publish(event driverproto.DriverEvent) bool {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	s.wake <- event
	return true
}

type codexWorkerHost struct {
	sink  *codexEventSink
	tools driverproto.ToolPort
}

func (h codexWorkerHost) GenerationLife() context.Context { return context.Background() }
func (h codexWorkerHost) Events() driverproto.EventSink   { return h.sink }
func (h codexWorkerHost) Logger() *slog.Logger            { return slog.New(slog.DiscardHandler) }
func (h codexWorkerHost) Tools() driverproto.ToolPort {
	if h.tools != nil {
		return h.tools
	}
	return codexTestToolPort{}
}
func (h codexWorkerHost) Resources() driverproto.ResourcePort { return nil }

type codexTestToolPort struct{}

func (codexTestToolPort) Catalog() []driverproto.ToolSpec {
	tools := metatool.MetaTools()
	out := make([]driverproto.ToolSpec, 0, len(tools))
	for _, tool := range tools {
		out = append(out, driverproto.ToolSpec{Name: tool.Spec.Name, Description: tool.Spec.Description, Schema: tool.Spec.Schema})
	}
	return out
}
func (codexTestToolPort) Invoke(context.Context, driverproto.WorkerTurnTarget, driverproto.ToolInvocation) driverproto.ToolResult {
	return driverproto.ToolResult{Text: `{"ok":true}`}
}

type codexFakeProcess struct {
	process *childProcess
	input   chan map[string]any
	out     *os.File
	mu      sync.Mutex
}

func newCodexFakeProcess(t *testing.T) *codexFakeProcess {
	t.Helper()
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	reaped := make(chan struct{})
	close(reaped)
	f := &codexFakeProcess{input: make(chan map[string]any, 64), out: stdoutW}
	f.process = &childProcess{stdin: stdinW, stdout: stdoutR, done: make(chan error), waitDone: make(chan struct{}), stderrDone: make(chan struct{}), reaped: reaped}
	go func() {
		scanner := bufio.NewScanner(stdinR)
		for scanner.Scan() {
			var frame map[string]any
			if json.Unmarshal(scanner.Bytes(), &frame) == nil {
				f.input <- frame
			}
		}
		_ = stdinR.Close()
		close(f.input)
	}()
	return f
}
func (f *codexFakeProcess) emit(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.out.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
}
func (f *codexFakeProcess) close() { f.mu.Lock(); _ = f.out.Close(); f.mu.Unlock() }

type codexHarness struct {
	t      *testing.T
	worker *worker
	sink   *codexEventSink
	proc   *codexFakeProcess
}

func newCodexHarness(t *testing.T, options driverproto.TurnOptions, inspectOpen func(map[string]any)) *codexHarness {
	t.Helper()
	h := &codexHarness{t: t, sink: newCodexEventSink()}
	cfg := Config{WorkspaceDir: t.TempDir(), Binary: "codex", Logger: slog.New(slog.DiscardHandler)}
	cfg.processFactory = func(context.Context, Config) (*childProcess, error) {
		h.proc = newCodexFakeProcess(t)
		return h.proc.process, nil
	}
	h.worker = newWorker(cfg, codexWorkerHost{sink: h.sink})
	h.worker.Open(context.Background(), driverproto.OpenRequest{Options: options})
	initialize := h.input()
	h.respond(initialize, map[string]any{"userAgent": "test"})
	initialized := h.input()
	if initialized["method"] != "initialized" {
		t.Fatalf("initialized=%v", initialized)
	}
	open := h.input()
	if inspectOpen != nil {
		inspectOpen(open)
	}
	// The real thread/start|resume response reports the session's actual
	// model/effort defaults alongside the thread (see appserver wire).
	h.respond(open, map[string]any{"thread": map[string]any{"id": "thread-1"}, "model": "gpt-session-default", "reasoningEffort": "session-effort"})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.WorkerReady); return ok })
	t.Cleanup(func() {
		h.worker.Retire()
		h.proc.close()
		select {
		case <-h.worker.Reaped():
		case <-time.After(time.Second):
			t.Error("worker did not reap")
		}
	})
	return h
}
func (h *codexHarness) input() map[string]any {
	h.t.Helper()
	select {
	case frame := <-h.proc.input:
		return frame
	case <-time.After(time.Second):
		h.t.Fatal("timed out waiting for rpc input")
		return nil
	}
}
func (h *codexHarness) respond(request map[string]any, result any) {
	h.t.Helper()
	h.proc.emit(h.t, map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": result})
}
func (h *codexHarness) notify(method string, params any) {
	h.proc.emit(h.t, map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}
func (h *codexHarness) waitEvent(match func(driverproto.DriverEvent) bool) driverproto.DriverEvent {
	h.t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-h.sink.wake:
			if match(event) {
				return event
			}
		case <-deadline:
			h.t.Fatal("timed out waiting for event")
			return nil
		}
	}
}
func rpcParams(frame map[string]any) map[string]any {
	params, _ := frame["params"].(map[string]any)
	return params
}

func (h *codexHarness) startTurn(attempt driverproto.AttemptToken, text string) driverproto.WorkerTurnTarget {
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: attempt, Messages: []driverproto.DriverMessage{{Text: text}}})
	request := h.input()
	h.respond(request, map[string]any{})
	turnID := fmt.Sprintf("turn-%d", attempt)
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": turnID, "status": "inProgress"}})
	return h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnStarted); return ok }).(driverproto.TurnStarted).Target
}

func TestOpenPassesSelectedModelToThreadStart(t *testing.T) {
	newCodexHarness(t, driverproto.TurnOptions{Model: "gpt-test", Effort: "high"}, func(open map[string]any) {
		if open["method"] != "thread/start" || rpcParams(open)["model"] != "gpt-test" {
			t.Fatalf("open=%v", open)
		}
	})
}

func TestDynamicToolCallUsesEntryTargetSnapshotAndNativeCallID(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{}, nil)
	target := h.startTurn(41, "run")
	port := &codexCapturePort{seen: make(chan codexCapturedInvocation, 1)}
	h.worker.host = codexWorkerHost{sink: h.sink, tools: port}
	params, _ := json.Marshal(map[string]any{"threadId": "thread-1", "turnId": string(target.Native), "callId": "native-call-9", "tool": "call_actor", "arguments": map[string]any{"actor_id": "tool:x", "type": "x.run"}})
	handler := h.worker.prepareServerRequest(h.worker.conn, "item/tool/call", params)
	h.worker.mu.Lock()
	h.worker.target = driverproto.WorkerTurnTarget{Attempt: 99, Native: "new-turn"}
	h.worker.mu.Unlock()
	result, rpcErr := handler()
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	seen := <-port.seen
	if seen.target != target || seen.invocation.CallID != "native-call-9" {
		t.Fatalf("captured=%+v want target=%+v", seen, target)
	}
	if result.(map[string]any)["success"] != true {
		t.Fatalf("result=%v", result)
	}
}

type codexCapturedInvocation struct {
	target     driverproto.WorkerTurnTarget
	invocation driverproto.ToolInvocation
}

type codexCapturePort struct{ seen chan codexCapturedInvocation }

func (p *codexCapturePort) Catalog() []driverproto.ToolSpec { return codexTestToolPort{}.Catalog() }
func (p *codexCapturePort) Invoke(_ context.Context, target driverproto.WorkerTurnTarget, invocation driverproto.ToolInvocation) driverproto.ToolResult {
	p.seen <- codexCapturedInvocation{target: target, invocation: invocation}
	return driverproto.ToolResult{Text: `{"ok":true}`}
}

func TestResumeSeedDigestMismatchCannotResume(t *testing.T) {
	seed := encodeResumeSeed("thread-1", "old-digest")
	if thread, ok := decodeResumeSeed(seed, "new-digest"); ok || thread != "thread-1" {
		t.Fatalf("mismatch decoded thread=%q ok=%v", thread, ok)
	}
	if thread, ok := decodeResumeSeed(seed, "old-digest"); !ok || thread != "thread-1" {
		t.Fatalf("matching seed thread=%q ok=%v", thread, ok)
	}
	if _, ok := decodeResumeSeed([]byte("legacy-thread-id"), "old-digest"); ok {
		t.Fatal("unverified legacy seed resumed")
	}
}

func TestSelectStickyEffortIsSentOnceOnNextTurnStart(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{}, nil)
	h.worker.mu.Lock()
	h.worker.usage.ContextTokens = 321
	h.worker.usage.ContextWindow = 999
	h.worker.mu.Unlock()
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 1, Kind: driverproto.TurnSelect, Options: driverproto.TurnOptions{Model: "gpt-cheap", Effort: "low"}})
	selected := h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok }).(driverproto.TurnEnded)
	if selected.Usage.ContextTokens != 321 || selected.Usage.ContextWindow != 999 {
		t.Fatalf("select usage=%+v", selected.Usage)
	}
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 2, Messages: []driverproto.DriverMessage{{Text: "one"}}})
	first := h.input()
	if rpcParams(first)["model"] != "gpt-cheap" || rpcParams(first)["effort"] != "low" {
		t.Fatalf("first turn=%v", first)
	}
	h.respond(first, map[string]any{})
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "inProgress"}})
	h.notify("turn/completed", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok })
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 3, Messages: []driverproto.DriverMessage{{Text: "two"}}})
	second := h.input()
	if _, ok := rpcParams(second)["model"]; ok {
		t.Fatalf("model repeated: %v", second)
	}
	if _, ok := rpcParams(second)["effort"]; ok {
		t.Fatalf("effort repeated: %v", second)
	}
}

func TestCompactStartMapsCompactionTurnToTurnEvents(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{}, nil)
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 4, Kind: driverproto.TurnCompact})
	request := h.input()
	if request["method"] != "thread/compact/start" || rpcParams(request)["threadId"] != "thread-1" {
		t.Fatalf("compact=%v", request)
	}
	h.respond(request, map[string]any{})
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "compact-turn", "status": "inProgress"}})
	started := h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnStarted); return ok }).(driverproto.TurnStarted)
	h.notify("thread/tokenUsage/updated", map[string]any{"threadId": "thread-1", "turnId": "compact-turn", "tokenUsage": map[string]any{"last": map[string]any{"totalTokens": 4540}, "modelContextWindow": 999}})
	h.notify("turn/completed", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "compact-turn", "status": "completed"}})
	ended := h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok }).(driverproto.TurnEnded)
	if started.Target != ended.Target || ended.Status != driverproto.TurnOK || ended.Usage.ContextTokens != 4540 || ended.Usage.ContextWindow != 999 {
		t.Fatalf("started=%+v ended=%+v", started, ended)
	}
}

func TestTokenUsageNotificationFillsTurnEndedUsage(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{Model: "gpt-test", Effort: "medium"}, nil)
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 5, Messages: []driverproto.DriverMessage{{Text: "usage"}}})
	request := h.input()
	h.respond(request, map[string]any{})
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-usage", "status": "inProgress"}})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnStarted); return ok })
	h.notify("thread/tokenUsage/updated", map[string]any{"threadId": "thread-1", "turnId": "turn-usage", "tokenUsage": map[string]any{"last": map[string]any{"totalTokens": 321}, "modelContextWindow": 999}})
	h.notify("turn/completed", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-usage", "status": "completed"}})
	ended := h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok }).(driverproto.TurnEnded)
	if ended.Usage.ContextTokens != 321 || ended.Usage.ContextWindow != 999 || ended.Usage.Model != "gpt-test" || ended.Usage.Effort != "medium" {
		t.Fatalf("usage=%+v", ended.Usage)
	}
}

// TestUnconfiguredUsageReportsSessionDefaults: with no decl-configured options,
// a turn runs on the session defaults codex reported in the thread/start
// response — usage accounting must name those actual values, never "".
func TestUnconfiguredUsageReportsSessionDefaults(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{}, nil)
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 3, Messages: []driverproto.DriverMessage{{Text: "hi"}}})
	request := h.input()
	h.respond(request, map[string]any{})
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-defaults", "status": "inProgress"}})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnStarted); return ok })
	h.notify("turn/completed", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-defaults", "status": "completed"}})
	ended := h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok }).(driverproto.TurnEnded)
	if ended.Usage.Model != "gpt-session-default" || ended.Usage.Effort != "session-effort" {
		t.Fatalf("usage must fall back to session defaults, got %+v", ended.Usage)
	}
}

// TestDynamicToolCallNarrationIsNotRepublished: dynamicToolCall items are the
// stream's re-telling of a host callback the host already projects; publishing
// them again would double every tool event on the ledger.
func TestDynamicToolCallNarrationIsNotRepublished(t *testing.T) {
	h := newCodexHarness(t, driverproto.TurnOptions{}, nil)
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: 4, Messages: []driverproto.DriverMessage{{Text: "hi"}}})
	request := h.input()
	h.respond(request, map[string]any{})
	h.notify("turn/started", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-tools", "status": "inProgress"}})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnStarted); return ok })
	h.notify("item/started", map[string]any{"threadId": "thread-1", "turnId": "turn-tools", "item": map[string]any{"id": "call-1", "type": "dynamicToolCall", "tool": "system_call", "status": "inProgress"}})
	h.notify("item/completed", map[string]any{"threadId": "thread-1", "turnId": "turn-tools", "item": map[string]any{"id": "call-1", "type": "dynamicToolCall", "tool": "system_call", "status": "completed"}})
	h.notify("item/started", map[string]any{"threadId": "thread-1", "turnId": "turn-tools", "item": map[string]any{"id": "call-2", "type": "commandExecution", "command": "ls", "status": "inProgress"}})
	h.notify("item/completed", map[string]any{"threadId": "thread-1", "turnId": "turn-tools", "item": map[string]any{"id": "call-2", "type": "commandExecution", "command": "ls", "status": "completed"}})
	h.notify("turn/completed", map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-tools", "status": "completed"}})
	h.waitEvent(func(event driverproto.DriverEvent) bool { _, ok := event.(driverproto.TurnEnded); return ok })
	h.sink.mu.Lock()
	var tools []driverproto.Tool
	for _, event := range h.sink.events {
		if tool, ok := event.(driverproto.Tool); ok {
			tools = append(tools, tool)
		}
	}
	h.sink.mu.Unlock()
	if len(tools) != 2 {
		t.Fatalf("want only codex-local pair, got %#v", tools)
	}
	for _, tool := range tools {
		if tool.CallID != "call-2" {
			t.Fatalf("dynamicToolCall narration republished: %#v", tool)
		}
	}
}
