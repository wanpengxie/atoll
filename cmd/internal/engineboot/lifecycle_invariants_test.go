package engineboot

import (
	"encoding/json"
	"github.com/wanpengxie/atoll/runtime/harness"
	"testing"
)

type terminalShape struct {
	Status    string          `json:"status"`
	ErrorCode string          `json:"error_code"`
	Detail    string          `json:"detail"`
	Value     json.RawMessage `json:"value"`
}

func decodeTerminal(t *testing.T, raw json.RawMessage) terminalShape {
	t.Helper()
	_, raw, _ = harness.UnwrapPayload(raw)
	var terminal terminalShape
	if err := json.Unmarshal(raw, &terminal); err != nil {
		t.Fatal(err)
	}
	return terminal
}
