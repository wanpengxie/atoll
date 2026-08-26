//go:build unix

package coderunner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
)

const (
	maxConcurrency = 8
	maxLogs        = 1 << 20
	// maxCapturedLogs bounds the logs carried in a terminal payload so the
	// envelope stays under the browser frame ceiling.
	maxCapturedLogs = 384 << 10
	termGrace       = 2 * time.Second
	programEnv      = "ATOLL_PROGRAM"
)

type logBuffer struct {
	mu       sync.Mutex
	entries  []logEntry
	bytes    int
	captured int
	exceeded chan struct{}
	once     sync.Once
}

func newLogBuffer() *logBuffer { return &logBuffer{exceeded: make(chan struct{})} }

func (b *logBuffer) add(stream, text string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bytes += len(text)
	if b.bytes > maxLogs {
		b.once.Do(func() { close(b.exceeded) })
		return
	}
	entry := logEntry{Stream: stream, Text: text}
	encoded, _ := json.Marshal(entry)
	if b.captured+len(encoded)+1 <= maxCapturedLogs {
		b.captured += len(encoded) + 1
		b.entries = append(b.entries, entry)
	}
}

func (b *logBuffer) snapshot() []logEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append(make([]logEntry, 0, len(b.entries)), b.entries...)
}

func (b *logBuffer) forceExceeded() { b.once.Do(func() { close(b.exceeded) }) }

type processRun struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	writeMu   sync.Mutex
	terminate sync.Once
	done      chan struct{}
	errMu     sync.Mutex
	waitErr   error
}

// startRuntime spawns the runtime with the program's path in the
// environment; stdin/stdout are the MCP session, stderr is captured as output.
func startRuntime(command string, args []string, cwd, programPath string) (*processRun, io.ReadCloser, io.ReadCloser, error) {
	cmd := exec.Command(command, args...)
	cmd.Dir = cwd
	cmd.Env = append(selectedEnvironment(), programEnv+"="+programPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, nil, err
	}
	run := &processRun{cmd: cmd, stdin: stdin, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		run.errMu.Lock()
		run.waitErr = err
		run.errMu.Unlock()
		close(run.done)
	}()
	return run, stdout, stderr, nil
}

func (p *processRun) err() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.waitErr
}

func selectedEnvironment() []string {
	var out []string
	for _, key := range []string{"PATH", "HOME", "LANG"} {
		if value, ok := os.LookupEnv(key); ok {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func (p *processRun) write(value any) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	_, err = p.stdin.Write(raw)
	return err
}

// stop ends the session the MCP stdio way — close the runtime's stdin, then
// SIGTERM the process group, then SIGKILL after the grace period.
func (p *processRun) stop() {
	p.terminate.Do(func() {
		p.writeMu.Lock()
		_ = p.stdin.Close()
		p.writeMu.Unlock()
		if p.cmd.Process == nil {
			return
		}
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		go func() {
			select {
			case <-p.done:
			case <-time.After(termGrace):
				_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			}
		}()
	})
}

// writeProgram materialises the program as a file the runtime imports; the
// stdio pair stays free for the protocol.
func (a *coderunnerActor) writeProgram(requestID string, program string, suffix string) (string, error) {
	dir := a.deps.WorkspaceDir
	if dir == "" {
		dir = os.TempDir()
	}
	dir = filepath.Join(dir, ".coderunner")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "run-"+sanitizeToolName(requestID)+suffix)
	if err := os.WriteFile(path, []byte(program), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (a *coderunnerActor) execute(sys actorbase.Sys, msg actorbase.Msg, spec runSpec, actors map[string]actor.ActorID) {
	table, err := a.buildToolTable(sys, msg, actors)
	if err != nil {
		_, _ = sys.Fail(msg, "dependency_missing", err.Error())
		return
	}
	command, args, suffix := a.cfg.runtime()
	programPath, err := a.writeProgram(string(msg.ID), spec.program, suffix)
	if err != nil {
		failWithFields(sys, msg, "runtime_failed", "write program: "+err.Error(), runtimeFailure{Kind: "exit", Message: err.Error()})
		return
	}
	defer os.Remove(programPath)
	cwd := a.deps.WorkspaceDir
	if cwd == "" {
		cwd = os.TempDir()
	}
	run, stdout, stderr, err := startRuntime(command, args, cwd, programPath)
	if err != nil {
		failWithFields(sys, msg, "runtime_failed", "start runtime "+command+": "+err.Error(), runtimeFailure{Kind: "exit", Message: err.Error()})
		return
	}
	a.logger().Info("coderunner node started", "request", string(msg.ID), "pid", run.cmd.Process.Pid, "runtime", command)
	logs := newLogBuffer()
	pending := newPendingSet()
	defer func() {
		pending.cancelAll()
		run.stop()
		select {
		case <-run.done:
		case <-time.After(termGrace + time.Second):
		}
	}()

	go scanStderr(stderr, logs)
	terminal := make(chan terminalFrame, 1)
	streamDone := make(chan struct{})
	host := &mcpHost{
		actor: a, sys: sys, msg: msg, spec: spec, actors: actors, table: table,
		run: run, logs: logs, pending: pending, terminal: terminal,
	}
	go host.serve(stdout, streamDone)

	outputLimit := func() {
		run.stop()
		pending.cancelAll()
		failWithFields(sys, msg, "output_limit", "execution logs exceeded 1 MiB", struct {
			Logs []logEntry `json:"logs"`
		}{Logs: logs.snapshot()})
	}
	select {
	case frame := <-terminal:
		pending.cancelAll()
		select {
		case <-logs.exceeded:
			outputLimit()
			return
		default:
		}
		if frame.failure != nil {
			frame.failure.Logs = logs.snapshot()
			failWithFields(sys, msg, "runtime_failed", frame.failure.Message, *frame.failure)
			return
		}
		_, _ = sys.Reply(msg, runResult{Value: frame.result, Logs: logs.snapshot()})
	case <-logs.exceeded:
		outputLimit()
	case <-msg.Ctx().Done():
		run.stop()
		pending.cancelAll()
		code := "cancelled"
		if errors.Is(msg.Ctx().Err(), context.DeadlineExceeded) {
			code = "timeout"
		}
		failWithFields(sys, msg, code, msg.Ctx().Err().Error(), struct {
			Logs []logEntry `json:"logs"`
		}{Logs: logs.snapshot()})
	case <-streamDone:
		// stdout may close just before Wait reports the process status.
		<-run.done
		waitErr := run.err()
		kind := "invalid_output"
		messageText := "runtime ended the session without a result"
		if waitErr != nil {
			kind = "exit"
			messageText = waitErr.Error()
		}
		failWithFields(sys, msg, "runtime_failed", messageText, runtimeFailure{Kind: kind, Message: messageText, Logs: logs.snapshot()})
	}
}

func scanStderr(reader io.Reader, logs *logBuffer) {
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			logs.add("stderr", string(buf[:n]))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logs.add("stderr", err.Error())
			}
			return
		}
	}
}

func allowedTarget(value string, actors map[string]actor.ActorID) (actor.ActorID, bool) {
	if target, ok := actors[value]; ok {
		return target, true
	}
	for _, target := range actors {
		if string(target) == value {
			return target, true
		}
	}
	return "", false
}
