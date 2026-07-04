package claudecode

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// emitFinal writes the single terminal agent.text (visibility=public) reply for
// one completed claude turn. Audience = the trigger sender (Erlang From routing).
func (b *Bridge) emitFinal(ctx context.Context, item turnItem, text string, turnIndex int) error {
	payload := map[string]any{
		"text":        text,
		"next_action": "done",
		"turn_index":  turnIndex,
	}
	return b.emitEnvelope(ctx, item, "agent.text", message.VisibilityPublic, payload)
}

// emitTerminalError surfaces an engine/LLM failure as a public terminal envelope
// and returns nil — the failure is now in the channel log, the turn ends, and
// the actor stays alive (only plumbing failures kill the cell).
func (b *Bridge) emitTerminalError(ctx context.Context, cause error, item turnItem, reason string) error {
	payload := map[string]any{
		"text":        fmt.Sprintf("claude bridge failed: %v", cause),
		"next_action": "failed",
		"reason":      reason,
	}
	_ = b.emitEnvelope(ctx, item, "agent.text", message.VisibilityPublic, payload)
	return nil
}

// emitEnvelope assembles + writes one event envelope addressed to the trigger
// sender.
func (b *Bridge) emitEnvelope(ctx context.Context, item turnItem, envType string, visibility message.Visibility, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("claude: marshal payload: %w", err)
	}
	now := b.cfg.NowFn()
	env, err := behavior.BuildEvent(
		func() time.Time { return time.UnixMilli(now) },
		behavior.EventSpec{
			ID:            b.envelopeID(now),
			Type:          envType,
			Payload:       body,
			Visibility:    visibility,
			Audience:      replyAudience(item.env.Sender.ID),
			ParentID:      item.env.ID,
			CorrelationID: item.correlationID(),
		})
	if err != nil {
		return err
	}
	env.TSReceived = now
	return b.write(ctx, *env)
}

func (b *Bridge) write(ctx context.Context, env message.Envelope) error {
	res, err := b.pen.Write(ctx, &env)
	if err != nil {
		return err
	}
	if !res.Accepted() {
		return fmt.Errorf("claude: emit rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return nil
}

func replyAudience(triggerSender actor.ActorID) message.Audience {
	if triggerSender == "" {
		return message.Audience{actor.SystemActorID}
	}
	return message.Audience{triggerSender}
}

func (b *Bridge) envelopeID(nowMs int64) message.ID {
	short := strings.TrimPrefix(string(b.self), "agent:")
	if short == "" {
		short = "anon"
	}
	return message.ID(fmt.Sprintf("claude-%s-%d-%d", short, nowMs, b.envelopeSeq.Add(1)))
}

// setCurrentTurn / currentRC thread the in-flight turn's trigger to the MCP tool
// handlers (which run on the SDK's ctx, not ours).
func (b *Bridge) setCurrentTurn(t *turnItem) {
	b.curMu.Lock()
	b.curTurn = t
	b.curMu.Unlock()
}

func (b *Bridge) currentRC() metatool.RuntimeContext {
	b.curMu.Lock()
	t := b.curTurn
	b.curMu.Unlock()
	if t == nil {
		return metatool.RuntimeContext{}
	}
	return metatool.RuntimeContext{
		Trigger: metatool.Trigger{Envelope: t.env, CorrelationID: t.env.CorrelationID},
	}
}

// checkpoint persists the claude session id into the durable state slot (the
// looper is the slot's only author; atoll stores it opaquely).
func (b *Bridge) checkpoint(sessionID string) {
	if b.cfg.Checkpoint == nil || sessionID == "" {
		return
	}
	if err := b.cfg.Checkpoint(json.RawMessage(sessionID)); err != nil {
		// best-effort: a failed checkpoint just means a cold start next boot.
		slog.Default().Warn("claude.checkpoint_session", "id", string(b.self), "err", err)
	}
}

// classifyAssistantError maps a claude AssistantMessageError into a coarse reason
// bucket for the failed terminal envelope.
func classifyAssistantError(e claude.AssistantMessageError) string {
	switch e {
	case claude.AssistantErrorAuthenticationFailed:
		return "llm_auth"
	case claude.AssistantErrorBillingError:
		return "llm_billing"
	case claude.AssistantErrorRateLimit:
		return "llm_rate_limit"
	case claude.AssistantErrorInvalidRequest:
		return "llm_invalid"
	case claude.AssistantErrorServerError:
		return "llm_server"
	default:
		return "llm_unknown"
	}
}

// --- mechanical self-answers (actor citizenship) -----------------------------

const agentDescription = "Claude Code agent: the channel's conversational brain (claude looper). Send it any request — it reasons over the channel context and orchestrates the channel's tool actors via call_actor."

const agentSkillDoc = "# agent (claude looper)\n\n" +
	"Conversational actor backed by the Claude Code engine. Accepts any kind=request " +
	"as a turn trigger (no closed type set), replies with agent.text events, and calls " +
	"other actors through the channel's meta tools.\n"

func (b *Bridge) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		_, ferr := behavior.Fail(ctx, b.pen, b.clock, env,
			"payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
		return ferr
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID:     string(b.self),
		Description: agentDescription,
		SkillDoc:    agentSkillDoc,
	}, req)
	if !ok {
		_, ferr := behavior.Fail(ctx, b.pen, b.clock, env,
			"type_unsupported", fmt.Sprintf("agent has no type %s", req.Type))
		return ferr
	}
	_, rerr := behavior.RespondJSON(ctx, b.pen, b.clock, env, answer)
	return rerr
}
