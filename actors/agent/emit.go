package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	kimierrors "github.com/wanpengxie/go-kimi/pkg/kimi/errors"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// emitTurnProgress writes one progress envelope summarising a completed
// step. Per impl-vocabulary §2.3 progress is `agent.text` carrying
// `visibility=system` (intermediate output / not delivered to view by
// default). Payload shape:
//
//	{
//	  "turn_index":  <1-based bridge turn>,
//	  "step_index":  <1-based within-turn step>,
//	  "tool_calls":  [{"name": "...", "preview": "..."}, ...],
//	}
//
// The progress envelope sits next to the eventual terminal agent.text
// (visibility=public) envelope under the same parent_id /
// correlation_id, so harness ordering keeps them grouped.
func (b *Bridge) emitTurnProgress(
	ctx context.Context,
	item turnItem,
	turnIndex int,
	stepIndex int,
	tools []wireToolCall,
) error {
	payload := map[string]any{
		"turn_index": turnIndex,
		"step_index": stepIndex,
	}
	if summary := summariseToolCalls(tools, 240); len(summary) > 0 {
		payload["tool_calls"] = summary
	}
	return b.emitEnvelope(ctx, item, "agent.text", message.VisibilitySystem, payload)
}

// emitTurnEnd writes the single terminal agent.text envelope for one
// completed Agent.Run. Per-step progress envelopes have already been
// emitted by handleWireMsg at each ToolCallResult boundary — this
// function only produces the final reply.
//
// `accumulated` is the full TextDelta-buffered string; the TurnEnd's
// own Output ContentParts (text + think) are preferred, falling back to
// the buffered stream when Output is empty (providers vary).
func (b *Bridge) emitTurnEnd(
	ctx context.Context,
	item turnItem,
	end wire.TurnEnd,
	accumulated string,
	turnIndex int,
) error {
	stop := strings.ToLower(strings.TrimSpace(end.StopReason))
	parts := parseOutputParts(end.Output)

	nextAction := "continue"
	switch stop {
	case "end_turn", "stop", "completed", "finish", "":
		nextAction = "done"
	case "max_tokens":
		nextAction = "max_tokens"
	case "tool_use":
		// go-kimi's soul aggregates tool steps internally and only
		// emits TurnEnd at Agent.Run completion. Seeing stop_reason=
		// tool_use at the bridge boundary means the run ended while
		// the LLM was still yielding — surface as `done` so the trigger
		// turn closes cleanly; per-step progress bubbles already gave
		// the UI visibility into what was attempted.
		nextAction = "done"
	}
	text := parts.text
	if text == "" {
		text = accumulated
	}
	payload := map[string]any{
		"text":        text,
		"next_action": nextAction,
		"stop_reason": end.StopReason,
		"turn_index":  turnIndex,
	}
	return b.emitEnvelope(ctx, item, "agent.text", message.VisibilityPublic, payload)
}

// emitEnvelope assembles + writes one event envelope. Audience is derived
// from the trigger sender — Erlang-style `From` routing: the agent always
// replies to whoever triggered it.
func (b *Bridge) emitEnvelope(
	ctx context.Context,
	item turnItem,
	envType string,
	visibility message.Visibility,
	payload map[string]any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kimi: marshal payload: %w", err)
	}
	env, err := b.buildAgentEvent(envType, visibility,
		replyAudience(item.env.Sender.ID), body,
		item.env.ID, item.correlationID())
	if err != nil {
		return err
	}
	return b.write(ctx, env)
}

// write commits one envelope through the harness chain; a reject is an
// error (the agent must know its emit did not land).
func (b *Bridge) write(ctx context.Context, env message.Envelope) error {
	res, err := b.writer.Write(ctx, &env)
	if err != nil {
		return err
	}
	if !res.Accepted() {
		return fmt.Errorf("kimi: emit rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return nil
}

// replyAudience returns the audience for an agent reply. Falls back to
// the system actor when the trigger sender id is empty (boot path /
// boot-failed terminal error).
func replyAudience(triggerSender actor.ActorID) message.Audience {
	if triggerSender == "" {
		return message.Audience{actor.SystemActorID}
	}
	return message.Audience{triggerSender}
}

// emitTerminalLLMError classifies the error, emits a failed terminal
// envelope, then wraps the underlying error as terminalEmittedError so
// the loop knows the failure already surfaced in the channel log.
// err == nil short-circuits to a no-op for convenience.
func (b *Bridge) emitTerminalLLMError(
	ctx context.Context,
	err error,
	parentEnvID message.ID,
	correlationID message.ID,
) error {
	if err == nil {
		return nil
	}
	reason := classifyLLMError(err)
	payload := map[string]any{
		"text":        fmt.Sprintf("llm bridge failed: %v", err),
		"next_action": "failed",
		"reason":      reason,
	}
	body, _ := json.Marshal(payload)
	// LLM-error terminal: emit as observation-only addressed to
	// system (no business actor fan-out). The originating trigger
	// sender is not available on this path.
	env, buildErr := b.buildAgentEvent("agent.text", message.VisibilityPublic,
		message.Audience{actor.SystemActorID}, body, parentEnvID, correlationID)
	if buildErr != nil {
		return errors.Join(err, buildErr)
	}
	if writeErr := b.write(ctx, env); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return terminalEmittedError{cause: err}
}

// classifyLLMError maps a go-kimi error into one of 5 reason buckets
// the agent emits as payload.reason on the failed terminal envelope.
// The mapping is deliberately coarse — UI handlers + operators care
// about retryable vs fatal, not provider-specific quirks.
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
	// network-shape errors — DNS / refused / timeout / tls — surface
	// as net.* types through the stdlib http stack.
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

// envelopeID generates a deterministic-shape id for emitted envelopes.
// The per-bridge sequence keeps multiple emits in the same millisecond
// unique while preserving the actor/time prefix for debugging.
func (b *Bridge) envelopeID(nowMs int64) message.ID {
	short := strings.TrimPrefix(string(b.self), "agent:")
	if short == "" {
		short = "anon"
	}
	return message.ID(fmt.Sprintf("kimi-%s-%d-%d", short, nowMs, b.envelopeSeq.Add(1)))
}

// buildAgentEvent assembles a kind=event envelope through the behavior
// builder (ONE home for event defaults), then stamps the binding-edge
// fields this path owns (deterministic per-actor id, TSReceived).
func (b *Bridge) buildAgentEvent(
	envType string,
	visibility message.Visibility,
	audience message.Audience,
	payload []byte,
	parentID message.ID,
	correlationID message.ID,
) (message.Envelope, error) {
	now := b.cfg.NowFn()
	env, err := behavior.BuildEvent(b.chID, b.sender(),
		func() time.Time { return time.UnixMilli(now) },
		behavior.EventSpec{
			ID:            b.envelopeID(now),
			Type:          envType,
			Payload:       payload,
			Visibility:    visibility,
			Audience:      audience,
			ParentID:      parentID,
			CorrelationID: correlationID,
		})
	if err != nil {
		return message.Envelope{}, err
	}
	env.TSReceived = now
	return *env, nil
}

// agentDescription / agentSkillDoc are the agent's actor.describe
// self-answer. The agent serves no request-type closed set — its surface is
// conversational (any request becomes an LLM turn), so Types stays empty and
// discovery guidance lives in the skill doc.
const agentDescription = "LLM agent: the channel's conversational brain. Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent\n\n" +
	"Conversational actor backed by an LLM. It accepts any kind=request as a " +
	"turn trigger (no closed type set), replies with agent.text events " +
	"(public terminal + system progress), and calls other actors through the " +
	"channel's meta tools.\n"

// handleDescribe serves the actor.describe self-answer through the standard
// introspect dispatch (mechanical — the LLM never sees reserved queries).
func (b *Bridge) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		_, ferr := behavior.Fail(ctx, b.writer, b.clock, env, b.sender(),
			"payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return ferr
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID:     string(b.self),
		Description: agentDescription,
		SkillDoc:    agentSkillDoc,
	}, req)
	if !ok {
		_, ferr := behavior.Fail(ctx, b.writer, b.clock, env, b.sender(),
			"type_unsupported", fmt.Sprintf("agent has no type %s", req.Type))
		return ferr
	}
	_, rerr := behavior.RespondJSON(ctx, b.writer, b.clock, env, b.sender(), answer)
	return rerr
}
