//go:build unix

package codex

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type childProcess struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdout     io.ReadCloser
	done       chan error
	waitDone   chan struct{}
	stderrDone chan struct{}
	reaped     chan struct{}
	once       sync.Once
}
type processFactory func(context.Context, Config) (*childProcess, error)

func spawnProcess(ctx context.Context, cfg Config) (*childProcess, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	gateKey := cfg.Binary + "@" + cfg.WorkspaceDir
	if err := waitSpawnGate(ctx, gateKey); err != nil {
		return nil, err
	}
	cmd := exec.Command(cfg.Binary, "app-server", "--stdio")
	cmd.Dir = cfg.WorkspaceDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return nil, err
	}
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()
		recordSpawnFailure(gateKey, err, cfg.Logger, time.Now())
		return nil, err
	}
	// The parent must not retain write ends. Unlike Cmd.StdoutPipe, these read
	// ends are not closed by Wait, so the RPC/stderr readers can drain every
	// final byte after process exit without racing the reaper.
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	recordSpawnSuccess(gateKey)
	p := &childProcess{cmd: cmd, stdin: stdin, stdout: stdout, done: make(chan error, 1), waitDone: make(chan struct{}), stderrDone: make(chan struct{}), reaped: make(chan struct{})}
	go func() {
		defer close(p.stderrDone)
		_, _ = io.Copy(logWriter{logger: cfg.Logger}, stderr)
		_ = stderr.Close()
	}()
	go func() { p.done <- cmd.Wait(); close(p.done); close(p.waitDone) }()
	go func() { <-p.waitDone; <-p.stderrDone; close(p.reaped) }()
	return p, nil
}

type logWriter struct {
	logger interface{ Warn(string, ...any) }
}

func (w logWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.logger.Warn("codex.app_server.stderr", "text", redactNative(string(p)))
	}
	return len(p), nil
}

func (p *childProcess) stop() {
	p.once.Do(func() {
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		if p.cmd == nil || p.cmd.Process == nil {
			return
		}
		pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = p.cmd.Process.Kill()
		}
	})
}

var gateMu sync.Mutex
var spawnGates = map[string]*spawnGate{}

type spawnGate struct {
	next     time.Time
	delay    time.Duration
	cause    string
	failures int
	lastLog  time.Time
	logs     int
}

func waitSpawnGate(ctx context.Context, key string) error {
	gateMu.Lock()
	g := spawnGates[key]
	if g == nil {
		g = &spawnGate{}
		spawnGates[key] = g
	}
	wait := time.Until(g.next)
	gateMu.Unlock()
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func recordSpawnFailure(key string, cause error, logger interface{ Warn(string, ...any) }, now time.Time) {
	gateMu.Lock()
	defer gateMu.Unlock()
	g := spawnGates[key]
	if g == nil {
		g = &spawnGate{}
		spawnGates[key] = g
	}
	causeText := fmt.Sprint(cause)
	if g.cause != "" && g.cause != causeText {
		g.delay = 0
		g.failures = 0
		g.lastLog = time.Time{}
	}
	g.cause = causeText
	g.failures++
	if g.delay == 0 {
		g.delay = time.Second
	} else {
		g.delay *= 2
	}
	if g.delay > 5*time.Minute {
		g.delay = 5 * time.Minute
	}
	g.next = now.Add(g.delay)
	if g.lastLog.IsZero() || now.Sub(g.lastLog) >= 5*time.Minute {
		g.lastLog = now
		g.logs++
		logger.Warn("codex.app_server.spawn_failed", "actor", key, "failures", g.failures, "retry_in", g.delay, "error", causeText)
	}
}
func recordSpawnSuccess(key string) { gateMu.Lock(); delete(spawnGates, key); gateMu.Unlock() }
