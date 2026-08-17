//go:build unix

package all

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/provider/claude"
	"github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type claudeRuntimeEvent struct {
	kind, code, text string
	op               runtimeproto.OpID
	status           runtimeproto.TurnStatus
}
type claudeRuntimeCollector struct {
	mu     sync.Mutex
	events []claudeRuntimeEvent
	wake   chan claudeRuntimeEvent
}

func newClaudeRuntimeCollector() *claudeRuntimeCollector {
	return &claudeRuntimeCollector{wake: make(chan claudeRuntimeEvent, 64)}
}
func (c *claudeRuntimeCollector) push(event claudeRuntimeEvent) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	c.wake <- event
}
func (c *claudeRuntimeCollector) TurnStarted(op runtimeproto.OpID, _ runtimeproto.TurnID) {
	c.push(claudeRuntimeEvent{kind: "started", op: op})
}
func (c *claudeRuntimeCollector) TurnRejected(op runtimeproto.OpID, code, detail string) {
	c.push(claudeRuntimeEvent{kind: "rejected", op: op, code: code, text: detail})
}
func (*claudeRuntimeCollector) Tool(runtimeproto.TurnID, runtimeproto.ToolEvent) {}
func (c *claudeRuntimeCollector) TurnEnded(_ runtimeproto.TurnID, status runtimeproto.TurnStatus, text, _ string, _ runtimeproto.TurnUsage) {
	c.push(claudeRuntimeEvent{kind: "ended", status: status, text: text})
}
func (*claudeRuntimeCollector) ControlDone(runtimeproto.OpID, runtimeproto.TurnID, runtimeproto.ControlVerdict, string) {
}
func (*claudeRuntimeCollector) ReadyDone(runtimeproto.OpID, runtimeproto.ReadyResult) {}
func (c *claudeRuntimeCollector) ProviderLost(_ runtimeproto.TurnID, _ runtimeproto.LostCause, detail string) {
	c.push(claudeRuntimeEvent{kind: "lost", text: detail})
}
func (*claudeRuntimeCollector) ResumeSeedUpdated([]byte) {}
func (c *claudeRuntimeCollector) RuntimeFault(code, detail string) {
	c.push(claudeRuntimeEvent{kind: "fault", code: code, text: detail})
}
func (c *claudeRuntimeCollector) await(t *testing.T, kind string) claudeRuntimeEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-c.wake:
			if event.kind == kind {
				return event
			}
			if event.kind == "rejected" || event.kind == "lost" || event.kind == "fault" {
				t.Fatalf("unexpected runtime event: %+v", event)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func TestRuntimeRetriesResumeInvalidWithFreshSession(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "spawns")
	wrapper := filepath.Join(dir, "claude-mock")
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_CLAUDE_MOCK_PROCESS=1\nexport CLAUDE_MOCK_RECORD=%q\nexec %q -test.run=TestClaudeMockProcessHelper -- \"$@\"\n", record, os.Args[0])
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg, err := claude.ParseConfig(nil, dir, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Binary = wrapper
	factory, _, err := runtime.Build(claude.NewProvider(cfg), runtime.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	events := newClaudeRuntimeCollector()
	rt, err := factory(runtimeproto.Deps{Parent: context.Background(), Logger: slog.New(slog.DiscardHandler)}, []byte("stale-session"), runtimeproto.TurnOptions{}, events)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Start(runtimeproto.StartCommand{Op: 1, Messages: []runtimeproto.Input{{Text: "hello"}}}); err != nil {
		t.Fatal(err)
	}
	events.await(t, "started")
	ended := events.await(t, "ended")
	if ended.status != runtimeproto.TurnStatusOK || ended.text != "retried" {
		t.Fatalf("ended=%+v", ended)
	}
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "--resume stale-session") || strings.Contains(lines[1], "--resume") || !strings.Contains(lines[1], "--session-id ") {
		t.Fatalf("spawn args=%q", lines)
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	ends, rejects := 0, 0
	for _, event := range events.events {
		if event.kind == "ended" {
			ends++
		}
		if event.kind == "rejected" {
			rejects++
		}
	}
	if ends != 1 || rejects != 0 {
		t.Fatalf("events=%+v", events.events)
	}
}

func TestClaudeMockProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CLAUDE_MOCK_PROCESS") != "1" {
		t.Skip("helper process")
	}
	separator := 0
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i + 1
			break
		}
	}
	args := os.Args[separator:]
	record := os.Getenv("CLAUDE_MOCK_RECORD")
	file, err := os.OpenFile(record, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(90)
	}
	_, _ = fmt.Fprintln(file, strings.Join(args, " "))
	_ = file.Close()
	resume := false
	for _, arg := range args {
		resume = resume || arg == "--resume"
	}
	encoder := json.NewEncoder(os.Stdout)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var frame map[string]any
		if json.Unmarshal(scanner.Bytes(), &frame) != nil {
			continue
		}
		if frame["type"] == "control_request" {
			request := frame["request"].(map[string]any)
			switch request["subtype"] {
			case "initialize":
				_ = encoder.Encode(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": frame["request_id"], "response": map[string]any{}}})
			case "get_context_usage":
				_ = encoder.Encode(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": frame["request_id"], "response": map[string]any{"totalTokens": 0, "maxTokens": 200000}}})
			}
			continue
		}
		if frame["type"] != "user" {
			continue
		}
		uuid := frame["uuid"].(string)
		if resume {
			_ = encoder.Encode(map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true, "num_turns": 0, "errors": []string{"No conversation found with session ID: stale-session"}})
			os.Exit(1)
		}
		_ = encoder.Encode(map[string]any{"type": "command_lifecycle", "command_uuid": uuid, "state": "queued"})
		_ = encoder.Encode(map[string]any{"type": "command_lifecycle", "command_uuid": uuid, "state": "started"})
		_ = encoder.Encode(map[string]any{"type": "system", "subtype": "init", "capabilities": []string{"interrupt_receipt_v1", "interrupt_cancel_queued_v1", "msg_lifecycle_v1"}, "claude_code_version": "2.1.233"})
		_ = encoder.Encode(map[string]any{"type": "result", "subtype": "success", "is_error": false, "result": "retried", "user_message_uuid": uuid, "terminal_reason": "completed"})
		_ = encoder.Encode(map[string]any{"type": "command_lifecycle", "command_uuid": uuid, "state": "completed"})
	}
	os.Exit(0)
}
