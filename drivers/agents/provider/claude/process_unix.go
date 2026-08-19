//go:build unix

package claude

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type childProcess struct {
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	exit        chan error
	waitDone    chan struct{}
	stderrDone  chan struct{}
	reaped      chan struct{}
	once        sync.Once
	stoppedByUs atomic.Bool
}

type processFactory func(context.Context, Config, []string) (*childProcess, error)

func spawnArgs(cfg Config, session string, resume bool, options driverproto.TurnOptions) []string {
	// --setting-sources "" : load no user/project/local settings — no user
	// plugins, hooks, custom commands, user MCP servers, no CLAUDE.md. Auth in
	// ~/.claude is untouched by this flag and keeps working. Built-in tools
	// stay on (they are the good part). --strict-mcp-config keeps any
	// project-scoped MCP registration for the cwd out as well.
	args := []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--setting-sources", "", "--strict-mcp-config"}
	if cfg.Prompt != "" {
		args = append(args, "--append-system-prompt", cfg.Prompt)
	}
	if resume {
		args = append(args, "--resume", session)
	} else {
		args = append(args, "--session-id", session)
	}
	model := options.Model
	if model == "" {
		model = cfg.Model
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	if options.Effort != "" {
		args = append(args, "--effort", options.Effort)
	}
	return args
}

func spawnProcess(ctx context.Context, cfg Config, args []string) (*childProcess, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	cmd := exec.Command(cfg.Binary, args...)
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
	cmd.Stdout, cmd.Stderr = stdoutWriter, stderrWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()
		cfg.Logger.Warn("claude.stream_json.spawn_failed", "error", err)
		return nil, err
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	p := &childProcess{cmd: cmd, stdin: stdin, stdout: stdout, exit: make(chan error, 1), waitDone: make(chan struct{}), stderrDone: make(chan struct{}), reaped: make(chan struct{})}
	go func() {
		defer close(p.stderrDone)
		_, _ = io.Copy(logWriter{logger: cfg.Logger}, stderr)
		_ = stderr.Close()
	}()
	go func() {
		p.exit <- cmd.Wait()
		close(p.exit)
		close(p.waitDone)
	}()
	go func() {
		<-p.waitDone
		<-p.stderrDone
		close(p.reaped)
	}()
	return p, nil
}

type logWriter struct {
	logger interface{ Debug(string, ...any) }
}

func (w logWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.logger.Debug("claude.stream_json.stderr", "text", redactNative(string(p)))
	}
	return len(p), nil
}

func (p *childProcess) stop() {
	p.once.Do(func() {
		p.stoppedByUs.Store(true)
		_ = p.stdin.Close()
		if p.cmd == nil || p.cmd.Process == nil {
			_ = p.stdout.Close()
			return
		}
		pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		go func() {
			timer := time.NewTimer(termGrace)
			defer timer.Stop()
			select {
			case <-p.waitDone:
				return
			case <-timer.C:
			}
			if err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = p.cmd.Process.Kill()
			}
		}()
	})
}
