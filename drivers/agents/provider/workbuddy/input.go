package workbuddy

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"strings"
)

func buildContent(batch []driverproto.DriverMessage, background []driverproto.ContextMessage, self driverproto.Situation) []map[string]any {
	out := make([]map[string]any, 0, len(batch)+1)
	if len(background) > 0 {
		var b strings.Builder
		b.WriteString("频道最近记录（可能与你已知重叠）：\n")
		for _, m := range background {
			b.WriteString(m.Text)
			b.WriteByte('\n')
		}
		out = append(out, map[string]any{"type": "text", "text": strings.TrimSpace(b.String())})
	}
	for _, m := range batch {
		text := m.Text
		if m.Caller.Actor != "" {
			text = driverproto.CallerLine(m.Caller) + "\n" + text
		}
		if line := driverproto.OriginLine(m.Origin); line != "" {
			text = line + "\n" + text
		}
		if fields := driverproto.FieldsLine(m.Payload); fields != "" {
			text += "\n" + fields
		}
		if lines := driverproto.AttachmentLines(m.Attachments, self); lines != "" {
			text += "\n" + lines
		}
		out = append(out, map[string]any{"type": "text", "text": text})
	}
	return out
}
