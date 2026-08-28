package codex

import (
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func buildInput(batch []driverproto.DriverMessage, background []driverproto.ContextMessage, self driverproto.Situation) []map[string]any {
	out := make([]map[string]any, 0, len(batch)+1)
	if len(background) > 0 {
		var b strings.Builder
		b.WriteString("频道最近记录（可能与你已知重叠）：\n")
		for _, i := range background {
			b.WriteString(i.Text)
			b.WriteByte('\n')
		}
		out = append(out, map[string]any{"type": "text", "text": strings.TrimSpace(b.String())})
	}
	for _, m := range batch {
		text := m.Text
		if m.Caller.Actor != "" {
			text = driverproto.CallerLine(m.Caller) + "\n" + text
		}
		// Where they said it, beside who said it. A person holds several
		// screens at once, and an agent asked to act on one has to be able to
		// name it.
		if line := driverproto.OriginLine(m.Origin); line != "" {
			text = line + "\n" + text
		}
		// A body is more than its text: the word's other fields are its
		// structured contract, and an agent that cannot see them cannot act on
		// them. They go after the text, like the caller line, rather than into
		// it — the person's words stay the person's words.
		if fields := driverproto.FieldsLine(m.Payload); fields != "" {
			text = text + "\n" + fields
		}
		if lines := driverproto.AttachmentLines(m.Attachments, self); lines != "" {
			text = text + "\n" + lines
		}
		out = append(out, map[string]any{"type": "text", "text": text})
	}
	return out
}
