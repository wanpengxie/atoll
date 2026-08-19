package claude

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/metatool"
)

type eventSink struct {
	mu     sync.Mutex
	events []driverproto.DriverEvent
	wake   chan driverproto.DriverEvent
}

func newEventSink() *eventSink { return &eventSink{wake: make(chan driverproto.DriverEvent, 4096)} }
func (s *eventSink) Publish(event driverproto.DriverEvent) bool {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	s.wake <- event
	return true
}
func (s *eventSink) snapshot() []driverproto.DriverEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]driverproto.DriverEvent(nil), s.events...)
}

type workerHost struct {
	sink  *eventSink
	tools driverproto.ToolPort
}

func (h workerHost) GenerationLife() context.Context { return context.Background() }
func (h workerHost) Events() driverproto.EventSink   { return h.sink }
func (h workerHost) Logger() *slog.Logger            { return slog.New(slog.DiscardHandler) }
func (h workerHost) Tools() driverproto.ToolPort {
	if h.tools != nil {
		return h.tools
	}
	return testToolPort{}
}
func (h workerHost) Resources() driverproto.ResourcePort { return nil }

type testToolPort struct{}

func (testToolPort) Catalog() []driverproto.ToolSpec {
	tools := metatool.MetaTools()
	out := make([]driverproto.ToolSpec, 0, len(tools))
	for _, tool := range tools {
		out = append(out, driverproto.ToolSpec{Name: tool.Spec.Name, Description: tool.Spec.Description, Schema: tool.Spec.Schema})
	}
	return out
}
func (testToolPort) Invoke(context.Context, driverproto.WorkerTurnTarget, driverproto.ToolInvocation) driverproto.ToolResult {
	return driverproto.ToolResult{Text: `{"ok":true}`}
}

type fakeProcess struct {
	process     *childProcess
	inputs      chan map[string]any
	out         *os.File
	exit        chan error
	once        sync.Once
	writeMu     sync.Mutex
	autoContext atomic.Bool
}

func newFakeProcess(t *testing.T) *fakeProcess {
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
	exit := make(chan error, 1)
	f := &fakeProcess{inputs: make(chan map[string]any, 64), out: stdoutW, exit: exit}
	f.autoContext.Store(true)
	f.process = &childProcess{stdin: stdinW, stdout: stdoutR, exit: exit, waitDone: make(chan struct{}), stderrDone: make(chan struct{}), reaped: reaped}
	go func() {
		scanner := bufio.NewScanner(stdinR)
		scanner.Buffer(make([]byte, 64<<10), maxLineBytes+1)
		for scanner.Scan() {
			var frame map[string]any
			if json.Unmarshal(scanner.Bytes(), &frame) == nil {
				if requestSubtype(frame) == "get_context_usage" && f.autoContext.Load() {
					id := requestID(frame)
					raw, _ := json.Marshal(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": id, "response": map[string]any{"totalTokens": 100, "maxTokens": 200000}}})
					f.writeMu.Lock()
					_, _ = f.out.Write(append(raw, '\n'))
					f.writeMu.Unlock()
					continue
				}
				f.inputs <- frame
			}
		}
		_ = stdinR.Close()
		close(f.inputs)
	}()
	return f
}

func (f *fakeProcess) emit(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	f.emitRaw(t, raw)
}
func (f *fakeProcess) emitRaw(t *testing.T, raw []byte) {
	t.Helper()
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if _, err := f.out.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		t.Fatal(err)
	}
}
func (f *fakeProcess) closeOutput() { f.once.Do(func() { _ = f.out.Close() }) }
func (f *fakeProcess) reportExit(err error) {
	select {
	case f.exit <- err:
	default:
	}
}

type harness struct {
	t       *testing.T
	worker  *worker
	sink    *eventSink
	proc    *fakeProcess
	args    []string
	cleanup sync.Once
}

func newHarness(t *testing.T, seed string) *harness {
	t.Helper()
	sink := newEventSink()
	h := &harness{t: t, sink: sink}
	cfg := Config{WorkspaceDir: t.TempDir(), Binary: "claude", Logger: slog.New(slog.DiscardHandler)}
	cfg.processFactory = func(_ context.Context, _ Config, args []string) (*childProcess, error) {
		h.args = append([]string(nil), args...)
		h.proc = newFakeProcess(t)
		return h.proc.process, nil
	}
	h.worker = newWorker(cfg, workerHost{sink: sink})
	h.worker.Open(context.Background(), driverproto.OpenRequest{ResumeSeed: []byte(seed)})
	t.Cleanup(h.close)
	return h
}

func (h *harness) close() {
	h.cleanup.Do(func() {
		h.worker.Retire()
		if h.proc != nil {
			h.proc.closeOutput()
			h.proc.reportExit(nil)
		}
		select {
		case <-h.worker.Reaped():
		case <-time.After(2 * time.Second):
			h.t.Error("worker did not reap")
		}
	})
}

func (h *harness) input() map[string]any {
	h.t.Helper()
	select {
	case frame := <-h.proc.inputs:
		return frame
	case <-time.After(2 * time.Second):
		h.t.Fatal("timed out waiting for stdin frame")
		return nil
	}
}

func (h *harness) noInputFor(wait time.Duration) {
	h.t.Helper()
	select {
	case frame := <-h.proc.inputs:
		h.t.Fatalf("unexpected stdin frame=%v", frame)
	case <-time.After(wait):
	}
}

func requestSubtype(frame map[string]any) string {
	request, _ := frame["request"].(map[string]any)
	value, _ := request["subtype"].(string)
	return value
}
func frameUUID(frame map[string]any) string {
	value, _ := frame["uuid"].(string)
	return value
}
func requestID(frame map[string]any) string {
	value, _ := frame["request_id"].(string)
	return value
}

func (h *harness) initializeSuccess() string {
	h.t.Helper()
	frame := h.input()
	if requestSubtype(frame) != "initialize" {
		h.t.Fatalf("first stdin frame=%v", frame)
	}
	id := requestID(frame)
	h.proc.emit(h.t, map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": id, "response": map[string]any{"commands": []any{}, "models": []any{}}}})
	return id
}

func (h *harness) start(attempt driverproto.AttemptToken, text string) string {
	h.t.Helper()
	h.worker.Start(context.Background(), driverproto.StartRequest{Attempt: attempt, Messages: []driverproto.DriverMessage{{Text: text}}})
	frame := h.input()
	if frame["type"] != "user" {
		h.t.Fatalf("start stdin frame=%v", frame)
	}
	return frameUUID(frame)
}

func (h *harness) wait(match func(driverproto.DriverEvent) bool) driverproto.DriverEvent {
	h.t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case event := <-h.sink.wake:
			if match(event) {
				return event
			}
		case <-deadline:
			h.t.Fatalf("timed out waiting for event; got %#v", h.sink.snapshot())
		}
	}
}

func waitAs[T driverproto.DriverEvent](h *harness) T {
	h.t.Helper()
	event := h.wait(func(event driverproto.DriverEvent) bool { _, ok := event.(T); return ok })
	return event.(T)
}

func eventsAs[T driverproto.DriverEvent](events []driverproto.DriverEvent) []T {
	var out []T
	for _, event := range events {
		if typed, ok := event.(T); ok {
			out = append(out, typed)
		}
	}
	return out
}

func goldenLines(t *testing.T, name string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), maxLineBytes+1)
	var lines [][]byte
	for scanner.Scan() {
		lines = append(lines, append([]byte(nil), scanner.Bytes()...))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func rewriteGolden(t *testing.T, raw []byte, ids map[string]string, requests map[string]string) []byte {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	value = rewriteValue(value, ids, requests)
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func rewriteValue(value any, ids, requests map[string]string) any {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if text, ok := child.(string); ok {
				if key == "command_uuid" || key == "user_message_uuid" {
					if replacement := ids[text]; replacement != "" {
						value[key] = replacement
					}
				}
				if key == "request_id" {
					if replacement := requests[text]; replacement != "" {
						value[key] = replacement
					}
				}
			}
			value[key] = rewriteValue(value[key], ids, requests)
		}
		return value
	case []any:
		for i, child := range value {
			if text, ok := child.(string); ok {
				if replacement := ids[text]; replacement != "" {
					value[i] = replacement
					continue
				}
			}
			value[i] = rewriteValue(child, ids, requests)
		}
		return value
	default:
		return value
	}
}

func emitGolden(t *testing.T, proc *fakeProcess, lines [][]byte, from, through int, ids, requests map[string]string) {
	t.Helper()
	if from < 1 || through > len(lines) || from > through {
		t.Fatalf("invalid golden range %d..%d of %d", from, through, len(lines))
	}
	for _, line := range lines[from-1 : through] {
		proc.emitRaw(t, rewriteGolden(t, line, ids, requests))
	}
}

func containsArgs(args []string, want ...string) bool {
	for i := 0; i+len(want) <= len(args); i++ {
		if reflect.DeepEqual(args[i:i+len(want)], want) {
			return true
		}
	}
	return false
}
