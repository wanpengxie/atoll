//go:build unix

package all

// Real-machine E2E for the production Claude provider. It is gated because
// it needs a logged-in Claude Code CLI, network access, and several model
// turns. Tool events are the steer/interrupt/crash barriers; no sleeps drive
// the protocol.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/provider/claude"
	"github.com/wanpengxie/atoll/drivers/agents/runtime"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

func TestClaudeLiveE2E(t *testing.T) {
	if os.Getenv("CLAUDE_E2E") != "1" {
		t.Skip("set CLAUDE_E2E=1 to run the live Claude Code E2E")
	}
	realClaude, err := exec.LookPath("claude")
	if err != nil {
		t.Fatalf("claude binary unavailable: %v", err)
	}
	workspace := t.TempDir()
	pidFile := filepath.Join(workspace, "claude-pids")
	wrapper := filepath.Join(workspace, "claude-recording-wrapper")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$$\" >> %q\nexec %q \"$@\"\n", pidFile, realClaude)
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg, err := claude.ParseConfig(nil, workspace, logger)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Binary = wrapper
	factory, spec, err := runtime.Build(claude.NewProvider(cfg), runtime.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("spec: caps=%+v receipt=%s", spec.Capabilities, spec.Bounds.ReceiptDeadline)

	events := newLiveCollector(t)
	rt, err := factory(runtimeproto.Deps{Parent: context.Background(), Logger: logger}, nil, events)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			rt.Close()
		}
	}()

	startTurn(t, rt, 1, "Reply with exactly PONG and nothing else. Do not run tools.")
	events.await(t, turnWait, "started")
	ended := events.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK || !strings.Contains(strings.ToUpper(ended.text), "PONG") {
		t.Fatalf("cold turn=%+v", ended)
	}
	startTurn(t, rt, 2, "Reply with the same word as last time, lowercase, and nothing else. Do not run tools.")
	events.await(t, turnWait, "started")
	ended = events.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK || !strings.Contains(strings.ToLower(ended.text), "pong") {
		t.Fatalf("reuse turn=%+v", ended)
	}

	seed := events.currentSeed()
	if len(seed) == 0 {
		t.Fatal("cold runtime did not publish a resume seed")
	}
	rt.Close()
	closed = true
	events2 := newLiveCollector(t)
	rt2, err := factory(runtimeproto.Deps{Parent: context.Background(), Logger: logger}, seed, events2)
	if err != nil {
		t.Fatal(err)
	}
	defer rt2.Close()
	startTurn(t, rt2, 3, "Recall the uppercase word from the first request. Reply with only that word. Do not run tools.")
	events2.await(t, turnWait, "started")
	ended = events2.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK || !strings.Contains(strings.ToUpper(ended.text), "PONG") {
		t.Fatalf("resume turn=%+v", ended)
	}

	startTurn(t, rt2, 4, "Use Bash to run exactly: sleep 45; echo BASE. After it finishes, reply with the output.")
	steerTurn := events2.await(t, turnWait, "started")
	events2.await(t, turnWait, "tool")
	steer := runtimeproto.Input{Text: "Also include the word BANANA in the same final answer."}
	if err := rt2.Control(runtimeproto.ControlCommand{Op: 5, Target: steerTurn.turn, Kind: runtimeproto.ControlSteer, Content: &steer}); err != nil {
		t.Fatal(err)
	}
	control, turnDone := false, false
	for !control || !turnDone {
		event := events2.await(t, turnWait, "control", "ended")
		if event.kind == "control" {
			control = true
			if event.verdict != runtimeproto.ControlAccepted {
				t.Fatalf("steer=%+v", event)
			}
		} else {
			turnDone = true
			if event.status != runtimeproto.TurnStatusOK || !strings.Contains(strings.ToUpper(event.text), "BANANA") {
				t.Fatalf("steered turn=%+v", event)
			}
		}
	}

	startTurn(t, rt2, 6, "Use Bash to run exactly: sleep 90; echo SHOULD_NOT_FINISH. Then reply with the output.")
	interruptTurn := events2.await(t, turnWait, "started")
	events2.await(t, turnWait, "tool")
	if err := rt2.Control(runtimeproto.ControlCommand{Op: 7, Target: interruptTurn.turn, Kind: runtimeproto.ControlInterrupt}); err != nil {
		t.Fatal(err)
	}
	control, turnDone = false, false
	for !control || !turnDone {
		event := events2.await(t, turnWait, "control", "ended")
		if event.kind == "control" {
			control = true
			if event.verdict != runtimeproto.ControlAccepted {
				t.Fatalf("interrupt=%+v", event)
			}
		} else {
			turnDone = true
			if event.status != runtimeproto.TurnStatusInterrupted {
				t.Fatalf("interrupted turn=%+v", event)
			}
		}
	}

	startTurn(t, rt2, 8, "Use Bash to run exactly: sleep 120; echo CRASHED. Then reply with the output.")
	events2.await(t, turnWait, "started")
	events2.await(t, turnWait, "tool")
	pid := lastRecordedPID(t, pidFile)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill recorded Claude child %d: %v", pid, err)
	}
	events2.await(t, turnWait, "lost")
	startTurn(t, rt2, 9, "Reply with exactly OK and nothing else. Do not run tools.")
	events2.await(t, turnWait, "started")
	ended = events2.await(t, turnWait, "ended")
	if ended.status != runtimeproto.TurnStatusOK || !strings.Contains(strings.ToUpper(ended.text), "OK") {
		t.Fatalf("respawn turn=%+v", ended)
	}
}

func lastRecordedPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			lines := strings.Fields(string(raw))
			if len(lines) > 0 {
				pid, err := strconv.Atoi(lines[len(lines)-1])
				if err == nil {
					return pid
				}
			}
		}
		select {
		case <-deadline:
			t.Fatalf("no child PID recorded in %s", path)
		default:
		}
	}
}
