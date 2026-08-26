package sysactor

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// logbookLimitMax bounds system.log.recent in complete conversation turns.
// Twenty is what an agent waking up can usefully read; bytes are bounded by
// the reader's own budget (drivers/agents/base/catchup.go).
const logbookLimitMax = 20

// LogbookRecentRequest and LogbookRecentResponse are the wire shapes of the
// system actor query. limit counts complete conversation turns, not rows; the
// response is those turns' projected rows in ledger order (each turn's request,
// its terminal, and — for a still-open request — its latest provisional), so a
// reader can render it as a conversation without regrouping. Housekeeping
// words (actor.describe, agent.context, agent controls, system.*) are on the
// ledger but never in this answer: they are not what "what did I miss" means.
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
		_, _ = sys.Fail(msg, "invalid_args", "system.log.recent requires {limit:1..20} (complete conversation turns)")
		return
	}
	if s.recent == nil {
		_, _ = sys.Fail(msg, "provider_failed", "this channel has no ledger reader attached, so recent turns cannot be read here. This is a fact about how the channel was assembled, not a passing fault")
		return
	}
	window, err := s.recent(msg.Ctx(), req.Limit)
	if err != nil {
		_, _ = sys.Fail(msg, "provider_failed", "reading recent turns failed: "+err.Error()+"; the ledger is momentarily unreadable, so a retry may succeed")
		return
	}
	out := LogbookRecentResponse{Messages: make([]LogbookMessage, 0, len(window.Rows))}
	for _, row := range window.Rows {
		out.Messages = append(out.Messages, LogbookMessage{Seq: row.Seq, Envelope: row.Envelope})
	}
	_, _ = sys.Reply(msg, out)
}
