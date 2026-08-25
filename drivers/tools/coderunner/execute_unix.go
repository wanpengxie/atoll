//go:build unix

package coderunner

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	maxConcurrency = 8
	maxLogs        = 1 << 20
	// One browser subject frame is capped at 512 KiB. Execution still trips at
	// maxLogs, while the terminal's JSON-encoded log array stays below that
	// transport ceiling with room for the envelope and failure metadata.
	maxCapturedLogs = 384 << 10
	termGrace       = 2 * time.Second
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
	return append([]logEntry(nil), b.entries...)
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

func startNode(node, cwd string) (*processRun, io.ReadCloser, io.ReadCloser, error) {
	cmd := exec.Command(node, "--input-type=module", "-e", runnerSource)
	cmd.Dir = cwd
	cmd.Env = selectedEnvironment()
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

func (p *processRun) stop() {
	p.terminate.Do(func() {
		_ = p.write(map[string]string{"op": "cancel"})
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

type pendingSet struct {
	mu    sync.Mutex
	items map[int64]actorbase.Pending
}

func (p *pendingSet) reserve(id int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id <= 0 {
		return false
	}
	if _, exists := p.items[id]; exists {
		return false
	}
	p.items[id] = nil
	return true
}

func (p *pendingSet) attach(id int64, pending actorbase.Pending) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.items[id]; !exists {
		return false
	}
	p.items[id] = pending
	return true
}

func (p *pendingSet) take(id int64) actorbase.Pending {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := p.items[id]
	delete(p.items, id)
	return pending
}

func (p *pendingSet) cancelAll() {
	p.mu.Lock()
	items := p.items
	p.items = make(map[int64]actorbase.Pending)
	p.mu.Unlock()
	for _, pending := range items {
		if pending != nil {
			_ = pending.Cancel()
		}
	}
}

type terminalFrame struct {
	result  json.RawMessage
	failure *runtimeFailure
}

func (a *coderunnerActor) execute(sys actorbase.Sys, msg actorbase.Msg, spec runSpec, actors map[string]actor.ActorID) {
	run, stdout, stderr, err := startNode(a.cfg.Node, a.deps.WorkspaceDir)
	if err != nil {
		failWithFields(sys, msg, "runtime_failed", "start node: "+err.Error(), runtimeFailure{Kind: "exit", Message: err.Error()})
		return
	}
	a.logger().Info("coderunner node started", "request", string(msg.ID), "pid", run.cmd.Process.Pid)
	logs := newLogBuffer()
	pending := &pendingSet{items: make(map[int64]actorbase.Pending)}
	defer func() {
		pending.cancelAll()
		run.stop()
		select {
		case <-run.done:
		case <-time.After(termGrace + time.Second):
		}
	}()

	actorStrings := make(map[string]string, len(actors))
	for requirement, id := range actors {
		actorStrings[requirement] = string(id)
	}
	program := "data:text/javascript;base64," + base64.StdEncoding.EncodeToString([]byte(spec.program))
	if err := run.write(startFrame{
		Op: "start", Program: program, Args: spec.args, Actors: actorStrings,
		Self: string(sys.Self()), Channel: string(a.deps.ChannelID), RequestID: string(msg.ID),
	}); err != nil {
		failWithFields(sys, msg, "runtime_failed", "write runner start: "+err.Error(), runtimeFailure{Kind: "exit", Message: err.Error()})
		return
	}

	go scanStderr(stderr, logs)
	terminal := make(chan terminalFrame, 1)
	streamDone := make(chan struct{})
	go a.readFrames(sys, msg, spec, actors, run, stdout, logs, pending, terminal, streamDone)

	select {
	case frame := <-terminal:
		pending.cancelAll()
		select {
		case <-logs.exceeded:
			run.stop()
			failWithFields(sys, msg, "output_limit", "execution logs exceeded 1 MiB", struct {
				Logs []logEntry `json:"logs"`
			}{Logs: logs.snapshot()})
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
		run.stop()
		pending.cancelAll()
		failWithFields(sys, msg, "output_limit", "execution logs exceeded 1 MiB", struct {
			Logs []logEntry `json:"logs"`
		}{Logs: logs.snapshot()})
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
		messageText := "runner closed stdout without a result"
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

func (a *coderunnerActor) readFrames(
	sys actorbase.Sys,
	msg actorbase.Msg,
	spec runSpec,
	actors map[string]actor.ActorID,
	run *processRun,
	stdout io.Reader,
	logs *logBuffer,
	pending *pendingSet,
	terminal chan<- terminalFrame,
	done chan<- struct{},
) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 32*1024), 16<<20)
	semaphore := make(chan struct{}, maxConcurrency)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var frame nodeFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			logs.add("stdout", string(line))
			continue
		}
		switch frame.Op {
		case "call":
			semaphore <- struct{}{}
			go func(frame nodeFrame) {
				defer func() { <-semaphore }()
				a.handleCall(sys, msg, spec, actors, run, pending, frame)
			}(frame)
		case "log":
			stream := frame.Stream
			if stream != "stdout" && stream != "stderr" && stream != "log" {
				stream = "log"
			}
			logs.add(stream, frame.Text)
		case "progress":
			var value any
			if len(frame.Value) > 0 {
				_ = json.Unmarshal(frame.Value, &value)
			}
			_, _ = sys.Progress(msg, frame.Status, value)
		case "result":
			terminal <- terminalFrame{result: append(json.RawMessage(nil), frame.Value...)}
			return
		case "error":
			terminal <- terminalFrame{failure: &runtimeFailure{Kind: frame.Kind, Message: frame.Message, Stack: frame.Stack}}
			return
		default:
			logs.add("stdout", string(line))
		}
	}
	if err := scanner.Err(); err != nil {
		logs.add("stdout", err.Error())
		logs.forceExceeded()
	}
	close(done)
}

func (a *coderunnerActor) handleCall(
	sys actorbase.Sys,
	msg actorbase.Msg,
	spec runSpec,
	actors map[string]actor.ActorID,
	run *processRun,
	pending *pendingSet,
	frame nodeFrame,
) {
	if !pending.reserve(frame.ID) {
		_ = run.write(answerFrame{Op: "answer", ID: frame.ID, Error: &callError{Code: "invalid_call", Detail: "call id must be positive and unique while in flight"}})
		return
	}
	defer pending.take(frame.ID)
	target, ok := allowedTarget(frame.Target, actors)
	if !ok {
		_ = run.write(answerFrame{Op: "answer", ID: frame.ID, Error: &callError{Code: "undeclared_capability", Detail: frame.Target + " is not in requires"}})
		return
	}
	input := frame.Input
	if len(input) == 0 {
		input = json.RawMessage("null")
	}
	var pd actorbase.Pending
	var err error
	if spec.forward {
		pd, err = sys.CallFor(msg.Cause(), actorbase.EffectiveCaller(msg), target, frame.Type, input)
	} else {
		pd, err = sys.Call(msg.Cause(), target, frame.Type, input)
	}
	if err != nil {
		_ = run.write(answerFrame{Op: "answer", ID: frame.ID, Error: &callError{Code: "call_failed", Detail: err.Error()}})
		return
	}
	if !pending.attach(frame.ID, pd) {
		_ = pd.Cancel()
		return
	}
	wait := defaultCallDeadline
	if frame.DeadlineMS > 0 {
		if frame.DeadlineMS > math.MaxInt64/int64(time.Millisecond) {
			wait = time.Duration(math.MaxInt64)
		} else {
			wait = time.Duration(frame.DeadlineMS) * time.Millisecond
		}
	}
	wait = boundedWait(msg.Ctx(), wait)
	response, err := pd.Wait(msg.Ctx(), wait)
	if err != nil {
		_ = pd.Cancel()
		_ = run.write(answerFrame{Op: "answer", ID: frame.ID, Error: &callError{Code: "call_failed", Detail: err.Error()}})
		return
	}
	var outcome struct {
		Status string `json:"status"`
		message.Failure
	}
	_ = json.Unmarshal(response.Payload, &outcome)
	if outcome.Status != message.StatusCompleted {
		failure := outcome.Failure
		code := failure.ErrorCode
		if code == "" {
			code = "call_failed"
		}
		_ = run.write(answerFrame{Op: "answer", ID: frame.ID, Error: &callError{Code: code, Detail: failure.Detail}})
		return
	}
	_ = run.write(answerFrame{Op: "answer", ID: frame.ID, OK: true, Payload: response.Payload})
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
