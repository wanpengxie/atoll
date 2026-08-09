//go:build unix

package codex

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

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
