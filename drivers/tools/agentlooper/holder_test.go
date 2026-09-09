package agentlooper

import (
	"encoding/json"
	"testing"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/protocol/message"
)

func holderTestBody(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func TestSessionHolderIgnoresUnacceptedStartSender(t *testing.T) {
	rows := []ledgerRow{
		{Seq: 1, ID: "poison", Kind: message.KindRequest, Type: agentloop.TypeStart, Sender: "agent:other:1", Audience: message.Audience{"tool:evil:1"}, Body: holderTestBody(map[string]any{"session_id": "s", "turn_id": "poison"})},
		{Seq: 2, ID: "poison-failed", Parent: "poison", Kind: message.KindResponse, Body: holderTestBody(map[string]any{"status": "failed"})},
		{Seq: 3, ID: "start", Kind: message.KindRequest, Type: agentloop.TypeStart, Sender: "agent:controller:1", Audience: message.Audience{"tool:loop:1"}, Body: holderTestBody(map[string]any{"session_id": "s", "turn_id": "turn"})},
		{Seq: 4, ID: "accepted", Parent: "start", Kind: message.KindResponse, Body: holderTestBody(map[string]any{"disposition": "accepted"})},
	}
	if got := sessionHolder(rows); got != "tool:loop:1" {
		t.Fatalf("holder=%q", got)
	}
}
