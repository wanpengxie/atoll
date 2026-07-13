package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
)

// turnState tracks the in-flight signals the bridge observes inside a
// single Agent.Run wire stream. Each go-kimi soul "step" inside that run
// emits a batch of ToolCallRequest events, followed by ToolCallResult
// events as the tools complete. We use the first ToolCallResult of a
// batch as the boundary to flush one progress envelope (agent.text +
// visibility=system, see emitTurnProgress): the LLM finished one
// reasoning step (typed by `step_index`), is about to feed tool results
// back to the next inference, and the UI gets a process bubble so the
// user is not staring at silence for 30-60s.
//
// On TurnEnd the bridge emits the single terminal agent.text
// (visibility=public) envelope. Stream-level TextDelta keeps buffering
// into the final text — it is never emitted as envelope spam.
type turnState struct {
	textBuf      strings.Builder
	pendingTools []wireToolCall
	stepIndex    int
}

// consumeWire reads the wire stream and:
//   - buffers TextDelta into the final text accumulator,
//   - collects ToolCallRequest events into per-step batches,
//   - emits one progress envelope (agent.text + visibility=system) at
//     each step boundary (first ToolCallResult after a ToolCallRequest
//     batch),
//   - emits one terminal agent.text (visibility=public) envelope on
//     TurnEnd.
//
// LLM streaming chunks are a transport-layer artifact and MUST NOT leak
// into the protocol envelope layer (the One Law: business change = new
// message; a chunk is not a business change). A request gets one final
// response envelope; intermediate progress is the same `agent.text`
// type carrying `visibility=system`. Design: per-step progress + one
// terminal agent.text per turn.
func (e *engine) consumeWire(
	ctx context.Context,
	agentDone <-chan struct{},
	trigger base.Trigger,
	sink base.Sink,
) error {
	state := &turnState{}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-agentDone:
			// Agent returned. Drain any wire events still buffered in
			// the channel so a scripted TurnEnd that fires just before
			// agent.Run returns is not lost. Non-blocking drain —
			// anything the agent emitted is already in the buffer.
			for drained := true; drained; {
				select {
				case msg, ok := <-e.wireCh:
					if !ok {
						drained = false
						break
					}
					done, err := e.handleWireMsg(state, msg, sink)
					if err != nil {
						return err
					}
					if done {
						return nil
					}
				default:
					drained = false
				}
			}
			// No TurnEnd means no terminal Output was emitted. Treat it
			// as turn failure so the trigger is not silently swallowed.
			return errors.New("kimi: agent completed without TurnEnd")
		case msg, ok := <-e.wireCh:
			if !ok {
				return errors.New("kimi: wire channel closed before TurnEnd")
			}
			done, err := e.handleWireMsg(state, msg, sink)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

// handleWireMsg routes one wire message. Returns done=true only when
// TurnEnd fires (i.e. the turn is finished and the consumer should
// stop reading the wire stream). Tool call request/result events are
// folded into the per-step progress envelope as described in the
// consumeWire godoc.
func (e *engine) handleWireMsg(
	state *turnState,
	msg wire.WireMessage,
	sink base.Sink,
) (bool, error) {
	switch m := msg.(type) {
	case wire.TextDelta:
		state.textBuf.WriteString(m.Delta)
		return false, nil
	case wire.ToolCallRequest:
		state.pendingTools = append(state.pendingTools, wireToolCall{
			ID:        m.ToolCall.ID,
			Name:      m.ToolCall.Name,
			Arguments: toolCallArgumentsJSON(m.ToolCall.Arguments),
		})
		return false, nil
	case wire.ToolCallResult:
		// First result after a batch of requests marks the step boundary.
		// Flush one progress Output summarising the step's tool calls, then
		// clear the pending list so the next step starts fresh.
		if len(state.pendingTools) == 0 {
			return false, nil
		}
		state.stepIndex++
		if err := e.emitTurnProgress(sink, state.stepIndex, state.pendingTools); err != nil {
			return false, err
		}
		state.pendingTools = state.pendingTools[:0]
		return false, nil
	case wire.TurnEnd:
		// If the LLM yielded with tool_use but the tools never resolved
		// (e.g. provider error mid-step) we still flush any pending requests
		// as a progress Output so the UI shows what the agent attempted.
		if len(state.pendingTools) > 0 {
			state.stepIndex++
			if err := e.emitTurnProgress(sink, state.stepIndex, state.pendingTools); err != nil {
				return false, err
			}
			state.pendingTools = state.pendingTools[:0]
		}
		return true, e.emitTurnEnd(sink, m, state.textBuf.String())
	default:
		return false, nil
	}
}

// toolCallArgumentsJSON normalises go-kimi's `types.JsonType` (= any)
// argument blob into the json.RawMessage the bridge stores per pending
// tool call. Empty / nil arguments yield an empty RawMessage — the
// preview builder treats that as "no preview available".
func toolCallArgumentsJSON(args any) json.RawMessage {
	if args == nil {
		return nil
	}
	b, err := json.Marshal(args)
	if err != nil {
		return nil
	}
	return b
}

// composeUserInput turns the trigger envelope into the string passed
// to Agent.Run. Heuristic:
//   - if payload has a top-level "text" string, use it verbatim.
//   - else encode payload as compact JSON.
//   - prepend a 1-line sender label so the LLM knows who triggered it.
func composeUserInput(env message.Envelope) string {
	var bodyText string
	if len(env.Payload) > 0 {
		var asMap map[string]any
		if err := json.Unmarshal(env.Payload, &asMap); err == nil {
			if t, ok := asMap["text"].(string); ok && strings.TrimSpace(t) != "" {
				bodyText = t
			}
		}
		if bodyText == "" {
			bodyText = string(env.Payload)
		}
	}
	if bodyText == "" {
		bodyText = "(empty trigger payload)"
	}
	senderLabel := fmt.Sprintf("[trigger sender=%s id=%s type=%s]", env.Sender.ID, env.ID, env.Type)
	return senderLabel + "\n" + bodyText
}

// wireToolCall is the JSON-shape of one ToolCallPart we decode from the
// wire stream. Field names align with go-kimi's wire format so
// json.Unmarshal round-trips without a translator.
type wireToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// extractTurnEndText flattens a TurnEnd.Output slice into the public reply
// text: it concatenates every type=="text" part (and a text-bearing part
// whose discriminator the provider dropped). Tool calls and thinking are a
// live-stream concern (handleWireMsg's pendingTools feed the progress
// bubbles) — TurnEnd only yields the terminal text. Reflection-free JSON
// round-trip avoids pulling go-kimi's types surface into the import set.
func extractTurnEndText(parts any) string {
	if parts == nil {
		return ""
	}
	raw, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	var slice []struct {
		Type string `json:"type"`
		Text string `json:"text,omitempty"`
	}
	if err := json.Unmarshal(raw, &slice); err != nil {
		return ""
	}
	var textBuf strings.Builder
	for i := range slice {
		if slice[i].Type == "text" || (slice[i].Type != "think" && slice[i].Type != "tool_call" && slice[i].Text != "") {
			textBuf.WriteString(slice[i].Text)
		}
	}
	return textBuf.String()
}

// summariseToolCalls builds the `tool_calls` array carried on
// progress envelopes (agent.text + visibility=system). Each entry is
// `{name, preview}` where preview is a short truncated string built
// from the arguments JSON — enough for an operator skimming the
// channel log to recognise what the agent is doing without exposing
// the full payload.
func summariseToolCalls(tools []wireToolCall, maxPreview int) []map[string]string {
	if len(tools) == 0 {
		return nil
	}
	if maxPreview <= 0 {
		maxPreview = 200
	}
	out := make([]map[string]string, 0, len(tools))
	for i := range tools {
		entry := map[string]string{"name": tools[i].Name}
		preview := buildToolPreview(tools[i].Arguments)
		if preview != "" {
			if len(preview) > maxPreview {
				preview = preview[:maxPreview] + "…"
			}
			entry["preview"] = preview
		}
		out = append(out, entry)
	}
	return out
}

// buildToolPreview turns a tool_call.arguments JSON blob into a one-line
// preview. Heuristic: when the payload is an object, prefer the first
// string-valued field (covers shell.cmd / read_file.path / write_file.path
// shapes). Fall back to the raw JSON when no such field exists.
func buildToolPreview(args json.RawMessage) string {
	if len(args) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(args, &obj); err != nil {
		return strings.TrimSpace(string(args))
	}
	for _, key := range []string{"cmd", "command", "path", "file", "query", "input", "url"} {
		if v, ok := obj[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	// No conventional key — encode back as a single compact JSON line.
	compact, err := json.Marshal(obj)
	if err != nil {
		return strings.TrimSpace(string(args))
	}
	return string(compact)
}
