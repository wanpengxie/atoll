package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (e *engine) establishSession(ctx context.Context, c *connection, seed string) (string, error) {
	if seed != "" {
		id, err := resumeThread(ctx, c, seed)
		if err == nil {
			return id, nil
		}
		if isClosingError(err) {
			id, err = resumeThread(ctx, c, seed)
			if err == nil {
				return id, nil
			}
		}
		if !isInvalidResumeError(err) {
			return "", err
		}
	}
	raw, err := c.rpc.call(ctx, "thread/start", map[string]any{
		"approvalPolicy": "never",
		"sandbox":        "danger-full-access",
		"cwd":            e.cfg.WorkspaceDir,
	}, rpcTimeout)
	if err != nil {
		return "", err
	}
	id := threadIDFrom(raw)
	if id == "" {
		return "", errors.New("codex thread/start response missing thread.id")
	}
	return id, nil
}

func resumeThread(ctx context.Context, c *connection, id string) (string, error) {
	raw, err := c.rpc.call(ctx, "thread/resume", map[string]any{"threadId": id, "excludeTurns": true}, rpcTimeout)
	if err != nil {
		return "", err
	}
	got := threadIDFrom(raw)
	if got == "" {
		return "", fmt.Errorf("codex thread/resume response missing thread.id")
	}
	return got, nil
}

func threadIDFrom(raw json.RawMessage) string {
	var r struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	_ = json.Unmarshal(raw, &r)
	return strings.TrimSpace(r.Thread.ID)
}
