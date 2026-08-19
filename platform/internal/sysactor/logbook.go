package sysactor

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
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
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil || req.Limit < 1 || req.Limit > logbookLimitMax {
		_, _ = sys.Fail(msg, "invalid_args", "system.log.recent requires {limit:1..5}")
		return
	}
	if s.logbook == nil {
		_, _ = sys.Fail(msg, "provider_failed", "this channel has no ledger reader attached, so recent rows cannot be read here. This is a fact about how the channel was assembled, not a passing fault")
		return
	}

	maxSeq, err := s.logbook.MaxSeq(msg.Ctx())
	if err != nil {
		_, _ = sys.Fail(msg, "provider_failed", "reading the ledger head failed: "+err.Error()+"; the ledger is momentarily unreadable, so a retry may succeed")
		return
	}
	ring := make([]storespec.StoredRow, 0, req.Limit)
	var after int64
	for after < maxSeq {
		rows, err := s.logbook.ReadAfterSeq(msg.Ctx(), after, logbookPageSize)
		if err != nil {
			_, _ = sys.Fail(msg, "provider_failed", "reading a ledger page failed: "+err.Error()+"; the ledger is momentarily unreadable, so a retry may succeed")
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			after = row.Seq
			if !includeLogbookRow(row) {
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

func includeLogbookRow(row storespec.StoredRow) bool {
	env := row.Envelope
	return env.Kind == message.KindRequest || env.Kind == message.KindResponse
}
