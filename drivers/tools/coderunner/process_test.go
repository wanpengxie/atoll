//go:build unix

package coderunner

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"
)

func TestStopTerminatesRunnerProcessGroup(t *testing.T) {
	run, stdout, stderr, err := startNode(defaultNode, t.TempDir())
	if err != nil {
		t.Skipf("node unavailable: %v", err)
	}
	go io.Copy(io.Discard, stdout)
	go io.Copy(io.Discard, stderr)
	program := `export async function run(){ await new Promise(() => {}); }`
	if err := run.write(startFrame{
		Op: "start", Program: "data:text/javascript;base64," + base64.StdEncoding.EncodeToString([]byte(program)),
		Args: json.RawMessage("null"), Actors: map[string]string{}, Self: "tool:runner:1", Channel: "c", RequestID: "r",
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	run.stop()
	select {
	case <-run.done:
	case <-time.After(termGrace + time.Second):
		t.Fatal("runner did not exit after cancellation")
	}
	if err := syscall.Kill(-run.cmd.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process group %d remains after cancellation: %v", run.cmd.Process.Pid, err)
	}
}

func TestLogBufferStopsBeforeLimit(t *testing.T) {
	logs := newLogBuffer()
	logs.add("log", string(make([]byte, maxLogs)))
	logs.add("log", "overflow")
	select {
	case <-logs.exceeded:
	default:
		t.Fatal("log limit did not trip")
	}
	if got := logs.snapshot(); len(got) != 0 {
		t.Fatalf("oversized log should count toward the limit without entering the terminal: %d entries", len(got))
	}
}
