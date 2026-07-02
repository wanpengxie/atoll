package device

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// boundedBuffer keeps at most max bytes; further writes are counted but
// dropped so a runaway command cannot bloat the response envelope.
type boundedBuffer struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	room := b.max - b.buf.Len()
	if room <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > room {
		b.truncated = true
		p = p[:room]
	}
	b.buf.Write(p)
	return len(p), nil
}

// handleExec implements device.exec: run a bash command line inside the
// channel workspace. A non-zero exit code is a completed result; only a
// timeout or a spawn failure fails the request.
func (a *Actor) handleExec(ctx context.Context, env *message.Envelope) error {
	var p ExecPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if p.Command == "" {
		return a.fail(ctx, env, "payload_invalid", "device.exec: command required")
	}

	ws, err := a.channelWorkspace(env.ChannelID)
	if err != nil {
		return a.fail(ctx, env, "workspace_unavailable", err.Error())
	}
	cwd := ws
	if p.Cwd != "" {
		cwd, err = resolvePath(ws, p.Cwd)
		if err != nil {
			return a.fail(ctx, env, "path_invalid", fmt.Sprintf("cwd: %v", err))
		}
	}

	timeout := p.TimeoutMs
	if timeout <= 0 {
		timeout = DefaultExecTimeoutMs
	}
	if timeout > MaxExecTimeoutMs {
		timeout = MaxExecTimeoutMs
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "bash", "-c", p.Command)
	cmd.Dir = cwd
	stdout := &boundedBuffer{max: MaxStreamBytes}
	stderr := &boundedBuffer{max: MaxStreamBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	start := a.clock()
	runErr := cmd.Run()
	durationMs := a.clock().Sub(start).Milliseconds()

	if runCtx.Err() == context.DeadlineExceeded {
		return a.fail(ctx, env, "exec_timeout",
			fmt.Sprintf("command timed out after %dms", timeout))
	}
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return a.fail(ctx, env, "exec_spawn_failed", runErr.Error())
		}
	}

	return a.respond(ctx, env, ExecResult{
		ExitCode:   exitCode,
		Stdout:     stdout.buf.String(),
		Stderr:     stderr.buf.String(),
		DurationMs: durationMs,
		Truncated:  stdout.truncated || stderr.truncated,
	})
}
