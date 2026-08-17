//go:build unix

package claude

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcessWaitDoesNotCloseStdoutBeforeFinalDrain(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "final-output")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf 'final-json-without-newline'\nprintf 'final-stderr' >&2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	p, err := spawnProcess(context.Background(), Config{Binary: binary, WorkspaceDir: dir, Logger: slog.New(slog.DiscardHandler)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-p.exit; err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(p.stdout)
	if err != nil || string(raw) != "final-json-without-newline" {
		t.Fatalf("stdout=%q err=%v", raw, err)
	}
}

func TestStopTermsThenKillsProcessGroupIncludingGrandchildren(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "term-resistant")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ntrap '' TERM\nsleep 300 &\necho $!\nwait\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	p, err := spawnProcess(context.Background(), Config{Binary: binary, WorkspaceDir: dir, Logger: slog.New(slog.DiscardHandler)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(p.stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	parentPID := p.cmd.Process.Pid
	p.stop()
	select {
	case <-p.reaped:
	case <-time.After(termGrace + 2*time.Second):
		t.Fatal("process group did not reap")
	}
	// The grandchild is reparented when its shell dies and lingers as a
	// zombie until init reaps it; a zombie still answers signal 0. Poll for
	// "gone or zombie" instead of asserting on the instant reaped closes.
	deadline := time.Now().Add(2 * time.Second)
	for _, pid := range []int{parentPID, childPID} {
		for {
			if gone(pid) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("pid %d remains", pid)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func gone(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil && errors.Is(err, syscall.ESRCH) {
		return true
	}
	status, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return true
	}
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "State:") {
			return strings.Contains(line, "Z")
		}
	}
	return false
}
