package agentmain

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func mainTestBody(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func TestClosedBoundariesUsesFirstAcceptedControllerAndHolderAtReport(t *testing.T) {
	items := []row{
		{Seq: 1, ID: "poison", Kind: message.KindRequest, Type: "loop.start", Sender: "agent:other:1", To: message.Audience{"tool:evil:1"}, Body: mainTestBody(map[string]any{"session_id": "s", "turn_id": "poison"})},
		{Seq: 2, ID: "poison-failed", Parent: "poison", Kind: message.KindResponse, Body: mainTestBody(map[string]any{"status": "failed"})},
		{Seq: 3, ID: "start", Kind: message.KindRequest, Type: "loop.start", Sender: "agent:controller:1", To: message.Audience{"tool:loop:1"}, Body: mainTestBody(map[string]any{"session_id": "s", "turn_id": "t1"})},
		{Seq: 4, ID: "accepted", Parent: "start", Kind: message.KindResponse, Body: mainTestBody(map[string]any{"disposition": "accepted"})},
		{Seq: 5, ID: "fake", Kind: message.KindRequest, Type: "loop.report", Sender: "tool:evil:1", Body: mainTestBody(map[string]any{"turn_id": "t1", "state": "completed"})},
		{Seq: 6, ID: "boundary", Kind: message.KindRequest, Type: "loop.report", Sender: "tool:loop:1", Body: mainTestBody(map[string]any{"turn_id": "t1", "state": "completed"})},
		{Seq: 7, ID: "unknown", Kind: message.KindRequest, Type: "loop.report", Sender: "tool:loop:1", Body: mainTestBody(map[string]any{"turn_id": "missing", "state": "completed"})},
	}

	boundaries := closedBoundaries(items)
	if len(boundaries) != 1 || boundaries[0].ID != "boundary" {
		t.Fatalf("boundaries=%+v", boundaries)
	}
}
