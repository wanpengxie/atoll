package agentlooper

import (
	"encoding/json"
	"fmt"
	"strings"
)

// validateHistory checks the model transcript, not delivery in the Actor ledger.
// IDs are scoped to an assistant batch; providers may reuse them in later turns.
// Unknown source history is rejected rather than silently rewritten.
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
			if reason := stopReason(raw); reason == "error" || reason == "aborted" {
				return fmt.Errorf("message %d is an incomplete assistant", i)
			}
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

func validArguments(raw json.RawMessage) bool {
	var value map[string]json.RawMessage
	return json.Unmarshal(raw, &value) == nil && value != nil
}

func stopReason(raw json.RawMessage) string {
	var m struct {
		Reason string `json:"stopReason"`
	}
	_ = json.Unmarshal(raw, &m)
	return strings.TrimSpace(m.Reason)
}
