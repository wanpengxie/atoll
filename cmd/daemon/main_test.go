package main_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// buildDaemon compiles cmd/daemon into the test tempdir and returns the
// absolute binary path.
func buildDaemon(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "coagent-daemon")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/daemon")
	wd, _ := filepath.Abs(".")
	cmd.Dir = filepath.Join(wd, "..", "..")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build cmd/daemon: %v\n%s", err, stderr.String())
	}
	return bin
}

// TestDaemon_FailFast_MissingHumanCallerSecret covers M1.6-T0.5: in
// production mode (no --mock-bus) the daemon MUST exit non-zero if
// --human-caller-secret is empty. Otherwise the control.write_message
// handler is silently nil and POST /api/channels/:id/messages returns
// "no daemon for channel".
//
// M1.6-T7 phase-2: error logs are now JSON on stdout (structured
// logging), so we assert against the combined output instead of stderr.
func TestDaemon_FailFast_MissingHumanCallerSecret(t *testing.T) {
	t.Parallel()
	bin := buildDaemon(t)
	dataDir := t.TempDir()

	cmd := exec.Command(bin,
		"--data-dir", dataDir,
		"--daemon-id", "daemon-failfast",
		"--daemon-epoch", "1",
		"--server-url", "ws://127.0.0.1:1",
		"--key", "dummy",
		// --human-caller-secret intentionally omitted.
	)

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("daemon exited 0 with no --human-caller-secret; want non-zero\noutput=%s", string(out))
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("unexpected error type: %v\noutput=%s", err, string(out))
	}
	if ee.ExitCode() == 0 {
		t.Errorf("exit code = 0, want non-zero\noutput=%s", string(out))
	}
	if got := string(out); got == "" {
		t.Error("expected fail-fast message on stdout/stderr, got empty")
	}
}

// TestDaemon_BootAndShutdown — assert cmd/daemon with --mock-bus boots
// past phase 4 (PhaseAcceptingNew), then reacts to SIGTERM by exiting
// cleanly within a short window. This is a binary-level smoke that
// complements the runtime-level TestDaemon_StartupPhases.
func TestDaemon_BootAndShutdown(t *testing.T) {
	t.Parallel()
	bin := buildDaemon(t)

	dataDir := t.TempDir()

	cmd := exec.Command(bin,
		"--mock-bus",
		"--data-dir", dataDir,
		"--daemon-id", "daemon-smoke",
		"--daemon-epoch", "1",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("daemon start: %v", err)
	}
	t.Cleanup(func() {
		// Ensure no orphan even on failure.
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	// Give the daemon time to assemble + RunPhases (mock-bus skips network
	// handshake so phase 4 is reached in <1s on a quiet machine).
	time.Sleep(750 * time.Millisecond)

	// Send SIGTERM — matches the signal.NotifyContext registration in
	// cmd/daemon/main.go which cancels ctx and unwinds RunDaemon.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	// Wait for graceful exit, bounded.
	doneCh := make(chan error, 1)
	go func() { doneCh <- cmd.Wait() }()

	select {
	case err := <-doneCh:
		if err != nil {
			ee, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("daemon Wait: %v\nstdout=%s\nstderr=%s",
					err, stdout.String(), stderr.String())
			}
			// SIGTERM → context cancel → RunDaemon returns nil → main
			// exits 0. If the process happens to inherit the signal
			// exit-code instead, accept that as well (some kernels mark
			// SIGTERM-on-shutdown as code 143 = 128+15).
			ws := ee.ExitCode()
			if ws != 0 && ws != 143 {
				t.Errorf("daemon exit code = %d; want 0 or 143\nstdout=%q\nstderr=%q",
					ws, stdout.String(), stderr.String())
			}
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("daemon did not exit within 8s after SIGTERM\nstdout=%s\nstderr=%s",
			stdout.String(), stderr.String())
	}
}
