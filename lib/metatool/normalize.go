package metatool

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// NormalizePayload ensures a raw JSON payload is a valid non-nil object.
func NormalizePayload(raw json.RawMessage) (json.RawMessage, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return CloneRawJSON(json.RawMessage(`{}`)), nil
	}
	if !json.Valid([]byte(text)) || text[0] != '{' {
		return nil, fmt.Errorf("channel tool payload must be a JSON object: %q", text)
	}
	return CloneRawJSON(json.RawMessage(text)), nil
}

// CloneRawJSON returns a deep copy of raw.
func CloneRawJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

// ResponseFailureReason reads a decoded response body, never a ledger payload.
func ResponseFailureReason(body map[string]any) string {
	if status := StringValue(body["status"]); strings.EqualFold(status, message.StatusFailed) {
		if reason := StringValue(body["reason"]); reason != "" {
			return reason
		}
		return message.StatusFailed
	}
	return ""
}

// StringValue extracts a trimmed string from an arbitrary value.
func StringValue(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

// ResultFromResponse builds a ResultValue from a response envelope. If
// the response is a failure, isError is true.
func ResultFromResponse(toolName string, env message.Envelope) (ResultValue, bool) {
	if env.Kind != message.KindResponse {
		return ResultValue{
			Name:    toolName,
			Value:   map[string]any{"error": fmt.Sprintf("channel tool got %s envelope %s", env.Kind, env.ID)},
			IsError: true,
		}, true
	}
	_, body, err := harness.UnwrapPayload(env.Payload)
	if err != nil {
		return NewError(toolName, InternalError, "invalid response payload envelope: "+err.Error(), "Inspect the responding adapter's payload format", nil), true
	}
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		return NewError(toolName, InternalError, "invalid response body: "+err.Error(), "Inspect the responding adapter's payload format", nil), true
	}
	if reason := ResponseFailureReason(value); reason != "" {
		return ResultValue{Name: toolName, Value: map[string]any{"error": reason, "payload": value}, IsError: true}, true
	}
	return ResultValue{Name: toolName, Value: value}, false
}
