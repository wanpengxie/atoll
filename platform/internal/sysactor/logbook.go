package sysactor

import (
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	logbookLimitMax = 5
	logbookPageSize = 256
)

// LogbookRecentRequest and LogbookRecentResponse are the wire shapes of the
// system actor query. Rows preserve the original envelope plus its log order.
type LogbookRecentRequest struct {
	Limit int `json:"limit"`
}

type LogbookRecentResponse struct {
	Messages []LogbookMessage `json:"messages"`
}

type LogbookMessage struct {
	Seq      int64            `json:"seq"`
	Envelope message.Envelope `json:"message"`
}

func (s *SystemActor) respondLogbookRecent(sys actorbase.Sys, msg actorbase.Msg) {
	var req LogbookRecentRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil || req.Limit < 1 || req.Limit > logbookLimitMax {
		_, _ = sys.Fail(msg, "payload_invalid", "logbook.recent requires {limit:1..5}")
		return
	}
	if s.logbook == nil {
		_, _ = sys.Fail(msg, "provider_failed", "logbook query unavailable")
		return
	}

	maxSeq, err := s.logbook.MaxSeq(msg.Ctx())
	if err != nil {
		_, _ = sys.Fail(msg, "provider_failed", err.Error())
		return
	}
	ring := make([]storespec.StoredRow, 0, req.Limit)
	var after int64
	for after < maxSeq {
		rows, err := s.logbook.ReadAfterSeq(msg.Ctx(), after, logbookPageSize)
		if err != nil {
			_, _ = sys.Fail(msg, "provider_failed", err.Error())
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			after = row.Seq
			if !includeLogbookRow(row, msg) {
				continue
			}
			if len(ring) == req.Limit {
				copy(ring, ring[1:])
				ring[len(ring)-1] = row
			} else {
				ring = append(ring, row)
			}
		}
		if len(rows) < logbookPageSize {
			break
		}
	}

	out := LogbookRecentResponse{Messages: make([]LogbookMessage, 0, len(ring))}
	for _, row := range ring {
		out.Messages = append(out.Messages, LogbookMessage{Seq: row.Seq, Envelope: row.Envelope})
	}
	_, _ = sys.Reply(msg, out)
}

func includeLogbookRow(row storespec.StoredRow, caller actorbase.Msg) bool {
	env := row.Envelope
	if env.Kind != message.KindRequest && env.Kind != message.KindResponse {
		return false
	}
	if env.Sender.ID == caller.Sender.ID {
		return false
	}
	if strings.HasPrefix(env.Type, "logbook.") {
		return false
	}
	// Responses to a logbook request inherit the request type in the current
	// response machinery; the prefix check above deliberately applies to both
	// request and response rows.
	return env.Type != platform.TypeLogbookRecent
}
