package agentlooper

import (
	"encoding/json"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"testing"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/protocol/message"
)

type rejectedStartSys struct {
	actorbase.Sys
	code  string
	posts int
}

func (s *rejectedStartSys) Fail(_ actorbase.Msg, code, _ string, _ ...map[string]any) (message.ID, error) {
	s.code = code
	return "rejected", nil
}
func (s *rejectedStartSys) Post(behavior.RequestSpec) (message.ID, error) {
	s.posts++
	return "report", nil
}

func TestCapacityRejectDoesNotEmitTerminalReport(t *testing.T) {
	sys := &rejectedStartSys{}
	l := &looper{cfg: Config{ControllerActor: "controller", LLMActor: "llm", MaxAssignments: 1}, active: map[string]*assignment{"busy": {}}}
	l.start(sys, internalRequest("start", agentloop.TypeStart, agentloop.StartRequest{SessionID: "s", TurnID: "rejected", ControllerActor: "agent:controller:1", Inputs: []agentloop.Input{{ID: "input", Text: "work"}}}))
	if sys.code != "capacity" || sys.posts != 0 || len(l.active) != 1 {
		t.Fatalf("code=%s posts=%d active=%d", sys.code, sys.posts, len(l.active))
	}
}

func TestTurnBoundaryRequiresAcceptanceBeforeReport(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		rows := []ledgerRow{
			{Seq: 1, ID: "initial", Kind: message.KindRequest, Type: agentloop.TypeStart, Sender: "agent:controller:1", Audience: message.Audience{"tool:loop:1"}, Body: holderTestBody(map[string]any{"session_id": "s", "turn_id": "initial"})},
			{Seq: 2, ID: "initial-ack", Parent: "initial", Kind: message.KindResponse, Body: holderTestBody(map[string]any{"disposition": "accepted"})},
			{Seq: 3, ID: "start", Kind: message.KindRequest, Type: agentloop.TypeStart, Sender: "agent:controller:1", Audience: message.Audience{"tool:loop:1"}, Body: holderTestBody(map[string]any{"session_id": "s", "turn_id": "t"})},
		}
		ack := ledgerRow{Seq: 4, ID: "ack", Parent: "start", Kind: message.KindResponse, Body: holderTestBody(map[string]any{"disposition": "accepted"})}
		if accepted {
			rows = append(rows, ack)
		}
		rows = append(rows, ledgerRow{Seq: 5, ID: "report", Kind: message.KindRequest, Type: agentloop.TypeReport, Sender: "tool:loop:1", Body: holderTestBody(map[string]any{"turn_id": "t", "state": "failed"})})
		if !accepted {
			ack.Seq = 6
			rows = append(rows, ack)
		}
		state := sessionTurnStates(rows)["t"]
		if state.closed != accepted {
			t.Fatalf("acceptance before report=%v state=%+v", accepted, state)
		}
	}
}

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
