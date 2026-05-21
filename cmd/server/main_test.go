package main_test

import (
	"bytes"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// buildServer compiles cmd/server into the test tempdir and returns the
// absolute binary path.
func buildServer(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "coagent-server")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/server")
	wd, _ := filepath.Abs(".")
	cmd.Dir = filepath.Join(wd, "..", "..")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build cmd/server: %v\n%s", err, stderr.String())
	}
	return bin
}

// pickFreePort grabs an unused localhost port. Race window with the
// subsequent bind is short enough for smoke tests.
func pickFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestServer_BootAndShutdown — assert cmd/server starts, reports
// http listen on stderr (its readiness signal via log.Printf), accepts
// a TCP dial to confirm the port is open, then exits cleanly on SIGTERM.
func TestServer_BootAndShutdown(t *testing.T) {
	t.Parallel()
	bin := buildServer(t)
	addr := pickFreePort(t)
	dbPath := filepath.Join(t.TempDir(), "server.db")

	// `--allow-dev-secrets` mirrors the dev / CI gate so the boot path
	// can run without injecting the 4 COAGENT_* env values (M1.5 FIX-T8
	// fail-fast). M1.6-T7 phase-1 keeps the gate honest in prod by
	// flipping gin into release mode when this flag is absent — the
	// smoke test stays on the dev branch to avoid leaking secrets into
	// CI logs.
	cmd := exec.Command(bin,
		"-addr", addr,
		"-db", dbPath,
		"-allow-dev-secrets",
	)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	stderrCh := make(chan struct{})
	var mu sync.Mutex
	go func() {
		buf := make([]byte, 1024)
		for {
			n, rerr := stderrPipe.Read(buf)
			if n > 0 {
				mu.Lock()
				stderr.Write(buf[:n])
				mu.Unlock()
			}
			if rerr != nil {
				if rerr != io.EOF {
					t.Logf("stderr read: %v", rerr)
				}
				close(stderrCh)
				return
			}
		}
	}()

	if err := cmd.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	// Poll stderr for the http-listen readiness line OR a successful
	// TCP dial to addr. Whichever wins first.
	deadline := time.Now().Add(8 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		mu.Lock()
		hasListen := strings.Contains(stderr.String(), "http listen")
		mu.Unlock()
		if hasListen {
			ready = true
			break
		}
		conn, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr == nil {
			_ = conn.Close()
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		mu.Lock()
		stderrStr := stderr.String()
		mu.Unlock()
		t.Fatalf("server didn't reach ready state within 8s\nstderr=%s", stderrStr)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	doneCh := make(chan error, 1)
	go func() { doneCh <- cmd.Wait() }()
	select {
	case err := <-doneCh:
		if err != nil {
			ee, ok := err.(*exec.ExitError)
			if !ok {
				mu.Lock()
				stderrStr := stderr.String()
				mu.Unlock()
				t.Fatalf("server Wait: %v\nstderr=%s", err, stderrStr)
			}
			ws := ee.ExitCode()
			if ws != 0 && ws != 143 {
				mu.Lock()
				stderrStr := stderr.String()
				mu.Unlock()
				t.Errorf("server exit code = %d want 0 or 143\nstderr=%s", ws, stderrStr)
			}
		}
	case <-time.After(8 * time.Second):
		mu.Lock()
		stderrStr := stderr.String()
		mu.Unlock()
		t.Fatalf("server did not exit within 8s after SIGTERM\nstderr=%s", stderrStr)
	}
	<-stderrCh
}

// TestServer_FailFastOnEmptySecrets is the M1.6-T7 phase-1 acceptance
// for the production fail-fast path. The server MUST exit 1 within a
// few hundred ms when the 4 COAGENT_* secrets are empty and the
// `--allow-dev-secrets` escape hatch is not set. The stderr message
// MUST surface the offending field name so an operator misconfiguring
// e.g. COAGENT_SESSION_SECRET can grep the cause without reading the
// gateway source.
func TestServer_FailFastOnEmptySecrets(t *testing.T) {
	t.Parallel()
	bin := buildServer(t)
	addr := pickFreePort(t)
	dbPath := filepath.Join(t.TempDir(), "server.db")

	cmd := exec.Command(bin,
		"-addr", addr,
		"-db", dbPath,
	)
	// Clear inherited env so the test process doesn't accidentally
	// supply the secrets through its own shell environment. Keep PATH
	// + HOME so the go runtime / sqlite extension loaders keep working.
	cmd.Env = []string{
		"PATH=" + envOrTest("PATH"),
		"HOME=" + envOrTest("HOME"),
		"COAGENT_GIN_MODE=test", // suppress gin debug banner in CI logs
	}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("server exited 0 with empty secrets; want fail-fast\nstdout/err=%s", string(out))
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("server Wait error not *ExitError: %v\nstdout/err=%s", err, string(out))
	}
	if code := ee.ExitCode(); code != 1 {
		t.Errorf("exit code = %d want 1\nstdout/err=%s", code, string(out))
	}
	combined := string(out)
	if !strings.Contains(combined, "SessionSecret") &&
		!strings.Contains(combined, "DaemonSharedSecret") &&
		!strings.Contains(combined, "DeviceTokenSecret") &&
		!strings.Contains(combined, "HumanCallerSecret") {
		t.Errorf("stderr did not surface offending secret field name\nstdout/err=%s", combined)
	}
}

func TestServer_FailFastOnMissingOriginAllowlists(t *testing.T) {
	t.Parallel()
	bin := buildServer(t)
	addr := pickFreePort(t)
	dbPath := filepath.Join(t.TempDir(), "server.db")

	cmd := exec.Command(bin,
		"-addr", addr,
		"-db", dbPath,
	)
	cmd.Env = []string{
		"PATH=" + envOrTest("PATH"),
		"HOME=" + envOrTest("HOME"),
		"COAGENT_GIN_MODE=test",
		"COAGENT_SESSION_SECRET=session-secret",
		"COAGENT_DAEMON_SECRET=daemon-secret",
		"COAGENT_DEVICE_SECRET=device-secret",
		"COAGENT_HUMAN_SECRET=human-secret",
	}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("server exited 0 with missing origin allowlists; want fail-fast\nstdout/err=%s", string(out))
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("server Wait error not *ExitError: %v\nstdout/err=%s", err, string(out))
	}
	if code := ee.ExitCode(); code != 1 {
		t.Errorf("exit code = %d want 1\nstdout/err=%s", code, string(out))
	}
	if combined := string(out); !strings.Contains(combined, "AllowedOrigins") {
		t.Errorf("stderr did not surface origin allowlist field\nstdout/err=%s", combined)
	}
}

// envOrTest is a tiny helper so the fail-fast test can build a clean
// child env without losing PATH/HOME. Standalone (not the cmd/server
// runtime's envOr) so the test stays independent of main.go internals.
func envOrTest(key string) string {
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, key+"=") {
			return strings.TrimPrefix(kv, key+"=")
		}
	}
	return ""
}
