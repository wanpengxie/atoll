package native

import (
	"encoding/json"
	"testing"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func (s *testSys) appendLedger(row logMessage, session string) {
	raw, _ := harness.WrapPayload(harness.Context{Session: session}, json.RawMessage(row.PayloadText))
	row.PayloadText, row.Seq = string(raw), int64(len(s.ledger)+1)
	s.ledger = append(s.ledger, row)
}

func deliverTestReport(c *controller, sys *testSys, msg actorbase.Msg) {
	var report agentloop.ReportRequest
	_ = json.Unmarshal(msg.Payload, &report)
	if report.SessionID == "" {
		for _, w := range c.data.Works {
			if w.ID == report.WorkID || w.AssignmentID == report.AssignmentID {
				report.SessionID = w.SessionID
				break
			}
		}
	}
	body, _ := json.Marshal(report)
	sys.appendLedger(logMessage{ID: msg.ID, Kind: message.KindRequest, MessageType: agentloop.TypeReport, Sender: msg.Sender, PayloadText: string(body)}, report.SessionID)
	c.handleReport(sys, msg)
}

func TestUnacceptedReportCannotCloseWorkOrBecomeBoundary(t *testing.T) {
	for _, receipt := range []string{"rejected", "missing", "late", "accepted"} {
		t.Run(receipt, func(t *testing.T) {
			sys := newTestSys(newTestState())
			c := &controller{cfg: Config{Loopers: []string{"loop-a"}, LLMActor: "llm", MaxOpenWorks: 8, MaxTurns: 4}, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
			c.handleAsk(sys, testRequest("ask", agentproto.TypeAsk, map[string]any{"text": "work", "delivery": "receipt", "submission_key": "ask", "session_id": "s"}))
			if len(c.data.Order) != 1 || len(sys.ledger) != 2 {
				t.Fatalf("start fixture failed: works=%v ledger=%v fails=%v", c.data.Order, sys.ledger, sys.fails)
			}
			w := c.data.Works[c.data.Order[0]]
			ack := sys.ledger[1]
			if receipt == "rejected" {
				raw, _ := harness.WrapPayload(harness.Context{Session: w.SessionID}, json.RawMessage(`{"status":"failed","error_code":"capacity"}`))
				ack.PayloadText = string(raw)
				sys.ledger[1] = ack
			} else if receipt != "accepted" {
				sys.ledger = sys.ledger[:1]
			}
			report := testReport("report", "tool:loop-a:9", agentloop.ReportRequest{SessionID: w.SessionID, WorkID: w.ID, TurnID: w.AssignmentID, State: "failed"})
			if receipt == "late" {
				sys.appendLedger(logMessage{ID: report.ID, Kind: message.KindRequest, MessageType: agentloop.TypeReport, Sender: report.Sender, PayloadText: string(report.Payload)}, w.SessionID)
				ack.Seq = 3
				sys.ledger = append(sys.ledger, ack)
				c.handleReport(sys, report)
			} else {
				deliverTestReport(c, sys, report)
			}
			if receipt == "accepted" {
				if w.State != agentproto.WorkClosed {
					t.Fatalf("accepted turn did not close: %+v", w)
				}
				return
			}
			if sys.fails["report"] != "turn_not_accepted" || w.State != agentproto.WorkOpen || w.BoundaryID != "" {
				t.Fatalf("unaccepted report changed work: failure=%s work=%+v", sys.fails["report"], w)
			}
			if err := c.refreshSessionRelations(sys); err != nil {
				t.Fatal(err)
			}
			if c.sessions[w.SessionID].LastBoundary != "" {
				t.Fatal("unaccepted report became projection boundary")
			}
			if receipt == "rejected" {
				// Even when the erroneous report arrives first, the capacity receipt
				// must still be able to return the work to the queue.
				c.cfg.Loopers = nil
				c.startDone(sys, startDone{Session: w.SessionID, Turn: w.AssignmentID, Looper: w.Looper, Error: "capacity", Cause: message.Root()})
				if w.State != agentproto.WorkOpen || w.Stage != "queued" || w.AssignmentID != "" {
					t.Fatalf("capacity receipt did not requeue: %+v", w)
				}
			}
		})
	}
}
