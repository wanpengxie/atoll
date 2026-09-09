package agentlooper

import (
	"encoding/json"
	"fmt"
	"strings"
)

// validateHistory accepts provider error/aborted assistant messages as durable
// context when every tool call in them is paired, just like a normal assistant.
func validateHistory(history []json.RawMessage) error {
	var pending []toolCall
	for i, raw := range history {
		var head struct {
			Role string `json:"role"`
			ID   string `json:"toolCallId"`
		}
		if json.Unmarshal(raw, &head) != nil {
			return fmt.Errorf("message %d is not an object", i)
		}
		if len(pending) > 0 {
			if head.Role != "toolResult" || head.ID != pending[0].ID {
				return fmt.Errorf("message %d does not close the next tool call", i)
			}
			pending = pending[1:]
			continue
		}
		switch head.Role {
		case "toolResult":
			return fmt.Errorf("message %d has no pending tool call", i)
		case "assistant":
			calls, _, err := assistantParts(raw)
			if err != nil {
				return fmt.Errorf("message %d: %w", i, err)
			}
			pending = calls
		case "user":
		default:
			return fmt.Errorf("message %d has unsupported role", i)
		}
	}
	if len(pending) > 0 {
		return fmt.Errorf("history has %d unclosed tool calls", len(pending))
	}
	return nil
}

func stopReason(raw json.RawMessage) string {
	var m struct {
		Reason string `json:"stopReason"`
	}
	_ = json.Unmarshal(raw, &m)
	return strings.TrimSpace(m.Reason)
}
