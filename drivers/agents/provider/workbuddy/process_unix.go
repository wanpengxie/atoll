//go:build unix

package workbuddy

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const stopGrace = 3 * time.Second

type childProcess struct {
	cmd                          *exec.Cmd
	stdin                        io.WriteCloser
	stdout                       io.ReadCloser
	exit                         chan error
	waitDone, stderrDone, reaped chan struct{}
	once                         sync.Once
	stopped                      atomic.Bool
}

type processFactory func(context.Context, Config) (*childProcess, error)

func spawnProcess(_ context.Context, cfg Config) (*childProcess, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	args := []string{"--acp", "--acp-transport", "stdio", "--strict-mcp-config", "--setting-sources", "", "--permission-mode", "bypassPermissions"}
	if cfg.Prompt != "" {
		args = append(args, "--append-system-prompt", cfg.Prompt)
	}
	cmd := exec.Command(cfg.Binary, args...)
	cmd.Dir = cfg.WorkspaceDir
	// This actor deliberately uses the desktop login/subscription. Shell-level
	// API-key overrides would silently select a different account and endpoint,
	// so remove only those competing auth knobs before installing WorkBuddy's
	// desktop profile variables. Token material itself is never copied.
	cmd.Env = append(envWithout(os.Environ(),
		"CODEBUDDY_API_KEY", "CODEBUDDY_AUTH_TOKEN", "CODEBUDDY_BASE_URL", "CODEBUDDY_CUSTOM_HEADERS", "CODEBUDDY_INTERNET_ENVIRONMENT",
		"CODEBUDDY_CONFIG_DIR", "WORKBUDDY_CONFIG_DIR", "WORKBUDDY_DATA_FOLDER_NAME", "ACC_PRODUCT_CONFIG_PATH", "CODEBUDDY_HOST", "CODEBUDDY_FORCE_HEADLESS_BUNDLE", "CODEBUDDY_API_KEY_HELPER_DISABLED",
	),
		"CODEBUDDY_CONFIG_DIR="+cfg.ConfigDir,
		"WORKBUDDY_CONFIG_DIR="+cfg.ConfigDir,
		"WORKBUDDY_DATA_FOLDER_NAME="+filepath.Base(cfg.ConfigDir),
		"ACC_PRODUCT_CONFIG_PATH="+filepath.Join(cfg.ConfigDir, "cache", "acc-product-config-v3.json"),
		"CODEBUDDY_HOST=workbuddy-desktop",
		"CODEBUDDY_FORCE_HEADLESS_BUNDLE=1",
		"CODEBUDDY_API_KEY_HELPER_DISABLED=1",
	)
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
		cfg.Logger.Warn("workbuddy.acp.spawn_failed", "error", err)
		return nil, err
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	p := &childProcess{cmd: cmd, stdin: stdin, stdout: stdout, exit: make(chan error, 1), waitDone: make(chan struct{}), stderrDone: make(chan struct{}), reaped: make(chan struct{})}
	go func() {
		defer close(p.stderrDone)
		_, _ = io.Copy(workbuddyLogWriter{cfg.Logger}, stderr)
		_ = stderr.Close()
	}()
	go func() { p.exit <- cmd.Wait(); close(p.exit); close(p.waitDone) }()
	go func() { <-p.waitDone; <-p.stderrDone; close(p.reaped) }()
	return p, nil
}

func envWithout(env []string, names ...string) []string {
	blocked := make(map[string]bool, len(names))
	for _, name := range names {
		blocked[name] = true
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if !blocked[name] {
			out = append(out, entry)
		}
	}
	return out
}

type workbuddyLogWriter struct{ logger *slog.Logger }

func (w workbuddyLogWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.logger.Debug("workbuddy.acp.stderr", "text", redactNative(string(p)))
	}
	return len(p), nil
}

func (p *childProcess) stop() {
	p.once.Do(func() {
		p.stopped.Store(true)
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
			t := time.NewTimer(stopGrace)
			defer t.Stop()
			select {
			case <-p.waitDone:
				return
			case <-t.C:
			}
			if err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = p.cmd.Process.Kill()
			}
		}()
	})
}
