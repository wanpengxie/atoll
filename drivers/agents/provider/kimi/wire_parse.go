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

// turnState tracks tool identities until their matching results arrive.
type turnState struct {
	pendingTools map[string]string
	pendingOrder []string
}

// consumeWire reads the wire stream and:
//   - discards TextDelta at the adapter boundary,
//   - maps tool request/results onto typed activity phases,
//   - writes the full TurnEnd value through the terminal sink.
//
// LLM streaming chunks are a transport-layer artifact and MUST NOT leak
// into the protocol envelope layer (the One Law: business change = new
// message; a chunk is not a business change).
func (e *engine) consumeWire(
	ctx context.Context,
	agentDone <-chan struct{},
	trigger base.Trigger,
	sink turnSink,
) error {
	state := &turnState{pendingTools: map[string]string{}}

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
			// No TurnEnd means no terminal value was reported. Treat it
			// as turn failure so the trigger is not silently swallowed.
			if err := endPendingTools(state, sink, "provider completed before tool result"); err != nil {
				return err
			}
			return errors.New("kimi: agent completed without TurnEnd")
		case msg, ok := <-e.wireCh:
			if !ok {
				if err := endPendingTools(state, sink, "wire closed before tool result"); err != nil {
					return err
				}
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
// stop reading the wire stream). Tool call request/result events map directly
// onto typed start/end activity phases.
func (e *engine) handleWireMsg(
	state *turnState,
	msg wire.WireMessage,
	sink turnSink,
) (bool, error) {
	switch m := msg.(type) {
	case wire.TextDelta:
		// Streaming chunks are transport artifacts. TurnEnd.Output is the full
		// value and is the only text allowed past this adapter boundary.
		return false, nil
	case wire.ToolCallRequest:
		callID := m.ToolCall.ID
		if callID == "" {
			callID = m.ID
		}
		state.pendingTools[callID] = m.ToolCall.Name
		state.pendingOrder = append(state.pendingOrder, callID)
		if err := sink.ToolStarted(toolActivity{CallID: callID, Tool: m.ToolCall.Name}); err != nil {
			return false, fmt.Errorf("%w: %v", errSinkWrite, err)
		}
		return false, nil
	case wire.ToolCallResult:
		callID := m.Result.ToolCallID
		if callID == "" {
			callID = m.ID
		}
		pendingTool, found := state.pendingTools[callID]
		if !found {
			return false, nil
		}
		tool := m.Result.Name
		if tool == "" {
			tool = pendingTool
		}
		status := "completed"
		if m.Result.IsError {
			status = "failed"
		}
		if err := sink.ToolEnded(toolActivity{CallID: callID, Tool: tool, Status: status}); err != nil {
			return false, fmt.Errorf("%w: %v", errSinkWrite, err)
		}
		delete(state.pendingTools, callID)
		return false, nil
	case wire.TurnEnd:
		if err := endPendingTools(state, sink, "turn ended before tool result"); err != nil {
			return false, err
		}
		return true, e.emitTurnEnd(sink, m)
	default:
		return false, nil
	}
}

func endPendingTools(state *turnState, sink turnSink, detail string) error {
	for _, callID := range state.pendingOrder {
		tool, found := state.pendingTools[callID]
		if !found {
			continue
		}
		if err := sink.ToolEnded(toolActivity{CallID: callID, Tool: tool, Status: "failed", Detail: detail}); err != nil {
			return fmt.Errorf("%w: %v", errSinkWrite, err)
		}
		delete(state.pendingTools, callID)
	}
	return nil
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

// extractTurnEndText flattens a TurnEnd.Output slice into the public reply
// text: it concatenates every type=="text" part (and a text-bearing part
// whose discriminator the provider dropped). Tool calls and thinking are a
// live-stream concern represented by typed activity; TurnEnd only yields the
// terminal text. Reflection-free JSON
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
