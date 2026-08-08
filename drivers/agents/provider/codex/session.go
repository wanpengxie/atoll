package codex

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func (w *worker) establishSession(ctx context.Context, c *connection, seed string) (string, driverproto.OpenResult) {
	if seed != "" {
		raw, err := c.rpc.call(ctx, "thread/resume", map[string]any{"threadId": seed, "excludeTurns": true}, rpcTimeout)
		if err == nil {
			if id := threadIDFrom(raw); id != "" {
				return id, driverproto.Ready()
			}
			return "", driverproto.OpenReject(driverproto.FailureProvider, "resume response missing thread id", driverproto.RetireWorker)
		}
		if isInvalidResumeError(err) {
			return "", driverproto.ResumeInvalid(err.Error())
		}
		return "", classifyOpen(err)
	}
	raw, err := c.rpc.call(ctx, "thread/start", map[string]any{"approvalPolicy": "never", "sandbox": "danger-full-access", "cwd": w.cfg.WorkspaceDir}, rpcTimeout)
	if err != nil {
		return "", classifyOpen(err)
	}
	id := threadIDFrom(raw)
	if id == "" {
		return "", driverproto.OpenReject(driverproto.FailureProvider, errors.New("thread start response missing thread id").Error(), driverproto.RetireWorker)
	}
	return id, driverproto.Ready()
}

func threadIDFrom(raw json.RawMessage) string {
	var v struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(raw, &v)
	return strings.TrimSpace(v.Thread.ID)
}
