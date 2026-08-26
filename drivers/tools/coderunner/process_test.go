//go:build unix

package coderunner

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestStopTerminatesRunnerProcessGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hang.mjs")
	if err := os.WriteFile(path, []byte(`export async function run(){ await new Promise(() => {}); }`), 0o600); err != nil {
		t.Fatal(err)
	}
	command, args, _ := Config{}.runtime()
	run, stdout, stderr, err := startRuntime(command, args, t.TempDir(), path)
	if err != nil {
		t.Skipf("node unavailable: %v", err)
	}
	go io.Copy(io.Discard, stdout)
	go io.Copy(io.Discard, stderr)
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
