package codex

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestInputLimitCountsUnicodeCharacters(t *testing.T) {
	for _, tt := range []struct {
		name, unit string
		count      int
		ok         bool
	}{
		{"ascii-n-1", "a", inputMaxChars - 1, true},
		{"ascii-n", "a", inputMaxChars, true},
		{"ascii-n+1", "a", inputMaxChars + 1, false},
		{"cjk-n", "界", inputMaxChars, true},
		{"cjk-n+1", "界", inputMaxChars + 1, false},
		{"emoji-n", "🙂", inputMaxChars, true},
		{"emoji-n+1", "🙂", inputMaxChars + 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildInput([]base.Trigger{{Envelope: message.Envelope{Payload: []byte(strings.Repeat(tt.unit, tt.count))}}}, nil)
			if (err == nil) != tt.ok {
				t.Fatalf("err=%v ok=%v", err, tt.ok)
			}
		})
	}
}
