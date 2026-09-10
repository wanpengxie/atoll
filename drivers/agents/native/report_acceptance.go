package native

import (
	"encoding/json"
	"errors"
	"sort"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// reportAcceptance projects acceptance at each report's ledger position. A later
// receipt cannot retroactively legitimize a report emitted before acceptance.
func reportAcceptance(sys actorbase.Sys, rows []logMessage, head int64) (map[message.ID]bool, error) {
	type turnKey struct{ session, turn string }
	starts := map[message.ID]turnKey{}
	accepted := map[turnKey]bool{}
	reports := map[message.ID]bool{}
	rows = append([]logMessage(nil), rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Seq < rows[j].Seq })
	for _, row := range rows {
		if row.MessageType != agentloop.TypeStart && row.MessageType != agentloop.TypeReport {
			continue
		}
		text := row.PayloadText

		app, body, err := harness.UnwrapPayload(json.RawMessage(text))
		if err != nil {
			return nil, err
		}
		switch {
		case row.Kind == message.KindRequest && row.MessageType == agentloop.TypeStart && sameSeat(sys.Self().String(), row.Sender.ID.String()):
			var start agentloop.StartRequest
			if json.Unmarshal(body, &start) != nil {
				continue
			}
			if start.SessionID == "" {
				start.SessionID = app.Session
			}
			if start.TurnID != "" && start.SessionID != "" {
				starts[row.ID] = turnKey{start.SessionID, start.TurnID}
			}
		case row.Kind == message.KindResponse && row.MessageType == agentloop.TypeStart:
			var ack struct{ Status, Disposition string }
			if json.Unmarshal(body, &ack) == nil && (ack.Status == "" || ack.Status == "completed") && (ack.Disposition == "accepted" || ack.Disposition == "already_accepted") {
				if turn, ok := starts[row.ParentID]; ok {
					accepted[turn] = true
				}
			}
		case row.Kind == message.KindRequest && row.MessageType == agentloop.TypeReport:
			var report agentloop.ReportRequest
			if json.Unmarshal(body, &report) != nil {
				continue
			}
			if report.SessionID == "" {
				report.SessionID = app.Session
			}
			if report.TurnID == "" {
				report.TurnID = report.AssignmentID
			}
			reports[row.ID] = accepted[turnKey{report.SessionID, report.TurnID}]
		}
	}
	return reports, nil
}

func reportHasAcceptedStart(sys actorbase.Sys, session string, report message.ID) (bool, error) {
	rows, head, err := nativeSessionRows(sys, session)
	if err != nil {
		return false, err
	}
	reports, err := reportAcceptance(sys, rows, head)
	if err != nil {
		return false, err
	}
	accepted, found := reports[report]
	if !found {
		return false, errors.New("report missing from session ledger")
	}
	return accepted, nil
}
