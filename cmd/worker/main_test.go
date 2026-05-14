package main_test

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildWorker compiles cmd/worker into the test tempdir and returns the
// absolute binary path.
func buildWorker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "coagent-worker")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/worker")
	wd, _ := filepath.Abs(".")
	cmd.Dir = filepath.Join(wd, "..", "..")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build cmd/worker: %v\n%s", err, stderr.String())
	}
	return bin
}

// TestWorker_MissingLeaseIDExitsTwo — without --lease-id and no
// COAGENT_WORKER_LEASE_ID env, the worker must emit a usage error to
// stderr and exit 2 (per cmd/worker/main.go).
func TestWorker_MissingLeaseIDExitsTwo(t *testing.T) {
	t.Parallel()
	bin := buildWorker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin)
	// Defensive: blank the env var if the developer happens to have it set.
	cmd.Env = append(cmd.Environ(), "COAGENT_WORKER_LEASE_ID=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("worker without --lease-id exited zero; want non-zero")
	}
	exit, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("err=%T want *exec.ExitError", err)
	}
	if exit.ExitCode() != 2 {
		t.Errorf("exit code=%d want 2", exit.ExitCode())
	}
	if !strings.Contains(stderr.String(), "lease-id") {
		t.Errorf("stderr missing lease-id hint; stderr=%q", stderr.String())
	}
}
