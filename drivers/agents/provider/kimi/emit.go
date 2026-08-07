package kimi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"
)

type toolActivity struct{ CallID, Tool, Status, Detail string }
type finalValue struct {
	Text, NextAction string
	Extra            map[string]any
}
type failure struct{ ErrorCode, Detail string }
type turnSink interface {
	ToolStarted(toolActivity) error
	ToolEnded(toolActivity) error
	Complete(finalValue) error
	Fail(failure) error
}

// errSinkWrite distinguishes collector plumbing faults from an ordinary
// missing TurnEnd, which is reported as a failed provider turn.
var errSinkWrite = errors.New("kimi: sink emit failed")

// emitTurnEnd writes the single full terminal value for one Agent.Run.
func (e *engine) emitTurnEnd(sink turnSink, end wire.TurnEnd) error {
	stop := strings.ToLower(strings.TrimSpace(end.StopReason))
	text := extractTurnEndText(end.Output)

	nextAction := "continue"
	switch stop {
	case "end_turn", "stop", "completed", "finish", "":
		nextAction = "done"
	case "max_tokens":
		nextAction = "max_tokens"
	case "tool_use":
		// go-kimi aggregates tool steps internally and only emits TurnEnd at
		// completion; a tool_use stop at this boundary means the run ended while
		// the LLM was still yielding — close cleanly as done.
		nextAction = "done"
	}
	if err := sink.Complete(finalValue{
		Text:       text,
		NextAction: nextAction,
		Extra:      map[string]any{"stop_reason": end.StopReason},
	}); err != nil {
		return fmt.Errorf("%w: %v", errSinkWrite, err)
	}
	return nil
}

// emitTerminalLLMError surfaces an LLM error as a failed terminal and returns
// nil on success; a collector write failure is propagated as errSinkWrite.
func (e *engine) emitTerminalLLMError(sink turnSink, err error) error {
	if err == nil {
		return nil
	}
	if emitErr := sink.Fail(failure{
		ErrorCode: classifyLLMError(err),
		Detail:    fmt.Sprintf("llm bridge failed: %v", err),
	}); emitErr != nil {
		return fmt.Errorf("%w: %v", errSinkWrite, emitErr)
	}
	return nil
}

// classifyLLMError maps a go-kimi error into one of a few coarse reason buckets
// (retryable vs fatal — UI/operators do not care about provider quirks).
func classifyLLMError(err error) string {
	if err == nil {
		return ""
	}
	var llmErr *kimierrors.LLMError
	if errors.As(err, &llmErr) {
		switch {
		case llmErr.StatusCode == 429:
			return "llm_rate_limit"
		case llmErr.StatusCode == 401 || llmErr.StatusCode == 403:
			return "llm_auth"
		case llmErr.StatusCode >= 500 && llmErr.StatusCode < 600:
			return "llm_server"
		case llmErr.StatusCode > 0:
			return "llm_unknown"
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "llm_network"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return "llm_network"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "llm_network"
	}
	return "llm_unknown"
}
