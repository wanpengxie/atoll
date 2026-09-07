package sysactor

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
)

func (s *SystemActor) respondLogbookQuery(sys actorbase.Sys, msg actorbase.Msg) {
	var req channelspec.LogQueryRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if err := req.Validate(); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if s.queryLog == nil {
		_, _ = sys.Fail(msg, "provider_failed", "this channel has no log query reader attached")
		return
	}
	ctx, cancel := context.WithTimeout(msg.Ctx(), 5*time.Second)
	defer cancel()
	out, err := s.queryLog(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, channelspec.ErrLogMessageNotFound):
			_, _ = sys.Fail(msg, "not_found", "message is absent, unreadable or outside head_seq in this channel; use coordinates from search results, not a different channel")
		case errors.Is(err, context.Canceled):
			_, _ = sys.Fail(msg, "cancelled", "log query was cancelled")
		case errors.Is(err, context.DeadlineExceeded):
			_, _ = sys.Fail(msg, "query_timeout", "log query exceeded its time budget; retry later with the same cursor, or a smaller limit if this search has many matches")
		default:
			_, _ = sys.Fail(msg, "provider_failed", "reading the channel log failed: "+err.Error())
		}
		return
	}
	_, _ = sys.Reply(msg, out)
}
