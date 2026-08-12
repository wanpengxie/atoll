package e2e

import (
	"testing"
	"time"
)

func TestServerCrashRestartPreservesIdentityAndLedger(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	messageID := ws.submit(c0ChannelID, "e2e.before-crash", "event", nil, map[string]any{
		"marker": "durable-across-kill-9",
	})
	ws.awaitEnvelope(func(envelope map[string]any) bool { return envelope["id"] == messageID }, 15*time.Second)

	h.restartServer()
	_, recovered := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	envelope := recovered.awaitEnvelope(func(envelope map[string]any) bool {
		return envelope["id"] == messageID
	}, 20*time.Second)
	payload, _ := envelope["payload"].(map[string]any)
	if payload["marker"] != "durable-across-kill-9" {
		t.Fatalf("replayed ledger row=%v", envelope)
	}
}
