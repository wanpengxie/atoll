package codex

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

func buildInput(batch []base.Trigger, background []base.ContextItem) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(batch)+1)
	if len(background) > 0 {
		var b strings.Builder
		b.WriteString("频道最近记录（可能与你已知重叠）：\n")
		for _, item := range background {
			b.WriteString(item.Rendered)
			b.WriteByte('\n')
		}
		out = append(out, map[string]any{"type": "text", "text": strings.TrimSpace(b.String())})
	}
	for _, tr := range batch {
		body := triggerBody(tr)
		if utf8.RuneCountInString(body) > inputMaxChars {
			return nil, errors.New("input_too_large")
		}
		out = append(out, map[string]any{"type": "text", "text": triggerText(tr)})
	}
	return out, nil
}
func triggerText(tr base.Trigger) string {
	body := triggerBody(tr)
	if tr.Envelope.Sender.ID != "" {
		return fmt.Sprintf("[from %s]\n%s", tr.Envelope.Sender.ID, body)
	}
	return body
}

func triggerBody(tr base.Trigger) string {
	// A present text field is authoritative even when empty — falling back to
	// the raw JSON for {"text":""} would make empty input look non-empty and
	// ship the literal braces to the model.
	var p struct {
		Text *string `json:"text"`
	}
	if json.Unmarshal(tr.Envelope.Payload, &p) == nil && p.Text != nil {
		return *p.Text
	}
	return string(tr.Envelope.Payload)
}
func steerExpected(tr base.Trigger, fallback string) string {
	var p struct {
		ExpectedTurnID string `json:"expected_turn_id"`
	}
	_ = json.Unmarshal(tr.Envelope.Payload, &p)
	if strings.TrimSpace(p.ExpectedTurnID) != "" {
		return strings.TrimSpace(p.ExpectedTurnID)
	}
	return fallback
}
