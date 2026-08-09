package codex

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func buildInput(batch []driverproto.DriverMessage, background []driverproto.ContextMessage) []map[string]any {
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
		if m.Sender != "" {
			text = fmt.Sprintf("[from %s]\n%s", m.Sender, text)
		}
		out = append(out, map[string]any{"type": "text", "text": text})
	}
	return out
}
