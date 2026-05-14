package main_test

import (
	"bytes"
	"io"
	"net"
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

	cmd := exec.Command(bin,
		"-addr", addr,
		"-db", dbPath,
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
