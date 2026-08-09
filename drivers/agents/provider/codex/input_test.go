package codex

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestInputHasNoProviderCharacterCeiling(t *testing.T) {
	text := strings.Repeat("🙂", (1<<20)+1)
	got := buildInput([]driverproto.DriverMessage{{Text: text}}, nil)
	if len(got) != 1 || got[0]["text"] != text {
		t.Fatal("large input was not preserved")
	}
}
