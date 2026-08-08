//go:build unix

package codex

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpawnGateBackoffAndLogRate(t *testing.T) {
	key := "actor:test-spawn-gate"
	gateMu.Lock()
	delete(spawnGates, key)
	gateMu.Unlock()
	defer recordSpawnSuccess(key)
	now := time.Unix(1000, 0)
	logger := slog.New(slog.DiscardHandler)
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for i, delay := range want {
		recordSpawnFailure(key, errors.New("missing binary"), logger, now.Add(time.Duration(i)*time.Minute))
		gateMu.Lock()
		got := spawnGates[key].delay
		gateMu.Unlock()
		if got != delay {
			t.Fatalf("failure %d delay=%v want=%v", i+1, got, delay)
		}
	}
	for i := 5; i < 20; i++ {
		recordSpawnFailure(key, errors.New("missing binary"), logger, now.Add(time.Duration(i)*time.Minute))
	}
	gateMu.Lock()
	g := *spawnGates[key]
	gateMu.Unlock()
	if g.delay != 5*time.Minute || g.logs != 4 {
		t.Fatalf("gate delay=%v logs=%d", g.delay, g.logs)
	}
}

func TestProcessWaitDoesNotCloseStdoutBeforeFinalDrain(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "final-output")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf 'final-json-without-newline'\nprintf 'final-stderr' >&2\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	p, err := spawnProcess(context.Background(), Config{
		Binary: binary, WorkspaceDir: dir,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(p.stdout)
	if err != nil || string(raw) != "final-json-without-newline" {
		t.Fatalf("stdout=%q err=%v", raw, err)
	}
}
