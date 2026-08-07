//go:build unix

package codex

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type childProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	done   chan error
	once   sync.Once
}
type processFactory func(context.Context, Config) (*childProcess, error)

func spawnProcess(ctx context.Context, cfg Config) (*childProcess, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	gateKey := cfg.ActorID
	if gateKey == "" {
		gateKey = cfg.Binary
	}
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
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		recordSpawnFailure(gateKey, err, cfg.Logger, time.Now())
		return nil, err
	}
	recordSpawnSuccess(gateKey)
	p := &childProcess{cmd: cmd, stdin: stdin, stdout: stdout, done: make(chan error, 1)}
	go func() { _, _ = io.Copy(logWriter{logger: cfg.Logger}, stderr) }()
	go func() { p.done <- cmd.Wait(); close(p.done) }()
	return p, nil
}

type logWriter struct {
	logger interface{ Warn(string, ...any) }
}

func (w logWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.logger.Warn("codex.app_server.stderr", "text", string(p))
	}
	return len(p), nil
}

func (p *childProcess) stop() {
	p.once.Do(func() {
		_ = p.stdin.Close()
		if p.cmd == nil || p.cmd.Process == nil {
			return
		}
		pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-p.done:
			return
		case <-time.After(2 * time.Second):
		}
		if pgid > 0 {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
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
