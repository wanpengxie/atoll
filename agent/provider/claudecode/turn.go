package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// turnItem is one mailbox envelope awaiting serial execution by the private loop.
type turnItem struct{ env message.Envelope }

func (t turnItem) correlationID() message.ID {
	return behavior.CorrelationID("", t.env.CorrelationID, t.env.ID)
}

// enqueueTurn pushes a turn without ever blocking: on overflow the oldest queued
// turn is evicted (newest input wins) and a system-visibility note records it.
func (b *Bridge) enqueueTurn(env message.Envelope) {
	item := turnItem{env: env}
	select {
	case b.turnQ <- item:
		return
	default:
	}
	var dropped turnItem
	select {
	case dropped = <-b.turnQ:
	default:
	}
	select {
	case b.turnQ <- item:
	default:
	}
	if dropped.env.ID != "" {
		payload := map[string]any{
			"text":        fmt.Sprintf("turn queue overflow: dropped oldest trigger %s (%s)", dropped.env.ID, dropped.env.Type),
			"next_action": "dropped",
		}
		_ = b.emitEnvelope(context.Background(), turnItem{env: dropped.env}, "agent.text", message.VisibilitySystem, payload)
	}
}

// runLoop is the private client edge — turns run strictly serially (one claude
// session per actor). A plumbing failure (emit rejected) records fatal + dies on
// the next contact; an engine/LLM error is surfaced as a terminal envelope and
// the loop continues (actor stays alive).
func (b *Bridge) runLoop(ctx context.Context) {
	defer b.loopWG.Done()
	turns := 0
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-b.turnQ:
			if !ok {
				return
			}
			turns++
			if err := b.runTurn(ctx, item, turns); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				b.fatalMu.Lock()
				b.fatal = err
				b.fatalMu.Unlock()
				return
			}
		}
	}
}

// runTurn drives one claude exchange: Query the input, drain ReceiveResponse
// (which ends at the ResultMessage), emit the terminal reply, and checkpoint the
// session id into the durable state slot. Tool calls the model makes resolve
// through the coagent MCP server (→ the held shell) mid-drain.
func (b *Bridge) runTurn(ctx context.Context, item turnItem, turnIndex int) error {
	b.setCurrentTurn(&item)
	defer b.setCurrentTurn(nil)

	input := composeUserInput(item.env)
	if err := b.client.Query(ctx, input); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return b.emitTerminalError(ctx, err, item, "claude_query")
	}

	var acc strings.Builder
	var sessionID string
	for msg := range b.client.ReceiveResponse(ctx) {
		switch m := msg.(type) {
		case *claude.AssistantMessage:
			if m.Error != "" {
				return b.emitTerminalError(ctx, fmt.Errorf("assistant error: %s", m.Error), item, classifyAssistantError(m.Error))
			}
			for _, block := range m.Content {
				if tb, ok := block.(*claude.TextBlock); ok && tb.Text != "" {
					acc.WriteString(tb.Text)
				}
			}
		case *claude.ResultMessage:
			sessionID = m.SessionID
			text := strings.TrimSpace(m.Result)
			if text == "" {
				text = acc.String()
			}
			if m.IsError {
				return b.emitTerminalError(ctx, fmt.Errorf("result error: %s", text), item, "claude_result")
			}
			if err := b.emitFinal(ctx, item, text, turnIndex); err != nil {
				return err // plumbing failure: propagate → positive death
			}
		}
	}
	if sessionID != "" {
		b.checkpoint(sessionID)
	}
	return nil
}

// composeUserInput renders a trigger envelope into the engine's user input,
// stamping the sender so the model knows whose message / result this is.
func composeUserInput(env message.Envelope) string {
	text := extractText(env.Payload)
	sender := string(env.Sender.ID)
	if sender == "" {
		return text
	}
	return fmt.Sprintf("[from %s]\n%s", sender, text)
}

func extractText(payload []byte) string {
	var p struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(payload, &p); err == nil && p.Text != "" {
		return p.Text
	}
	return string(payload)
}
