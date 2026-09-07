package home

import (
	"context"
	"slices"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Expand a hit on the shared channel timeline, without inheriting search
// filters. Other roles' replies, objections and parallel discussions matter.
func queryLogContext(ctx context.Context, reader storespec.VisibleExchangeQuery, q channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
	out := channelspec.LogQueryResponse{Turns: []channelspec.LogQueryTurn{}}
	anchor, found, err := reader.FindVisibleSeq(ctx, q.AroundSeq)
	if err != nil {
		return out, err
	}
	if !found || channelspec.HousekeepingWord(anchor.Envelope.Type) {
		return out, channelspec.ErrLogMessageNotFound
	}
	c := &channelspec.LogQueryContext{
		Before: []channelspec.LogQueryMessage{}, After: []channelspec.LogQueryMessage{},
	}
	out.Context = c
	radius := q.Radius
	if radius == 0 {
		radius = 3
	}
	head := q.HeadSeq
	p := &logProjection{ctx: ctx, reader: reader, view: q.View, cache: map[message.ID]projectedLogRow{}}
	for _, after := range []bool{false, true} {
		cursor := q.BeforeSeq
		if after {
			cursor = q.AfterSeq
		}
		if cursor == 0 {
			cursor = q.AroundSeq
		}
		items := []channelspec.LogQueryMessage{}
		scanned, bytes := 0, 0
		hasMore, limited := false, false
		for {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			page, err := reader.ReadVisibleWindow(ctx, cursor, head, after, logQueryBatch)
			if err != nil {
				return out, err
			}
			head = page.HeadSeq
			out.HeadSeq = head
			p.head = head
			hasMore = page.HasMore
			for i, row := range page.Rows {
				cursor = row.Seq
				scanned++
				out.Scanned++
				bytes += len(row.Envelope.Payload)
				hasMore = i+1 < len(page.Rows) || page.HasMore
				if !channelspec.HousekeepingWord(row.Envelope.Type) {
					v, err := p.row(row)
					if err != nil {
						return out, err
					}
					// Empty response packets describe state, not a neighbor's
					// utterance. Their state is attached to the request instead.
					if v.include && (q.View == "raw" || row.Envelope.Kind != message.KindResponse || v.text != "") {
						items = append(items, p.item(row, v, 0, logQueryExcerptChars))
					}
				}
				limited = hasMore && (scanned >= logQueryScanLimit || bytes >= logQueryScanBytes)
				if len(items) >= radius || limited {
					break
				}
			}
			if len(items) >= radius || limited || !hasMore {
				break
			}
		}
		if after {
			c.After, c.NextAfterSeq, c.HasNewer, c.AfterScanLimited = items, cursor, hasMore, limited
		} else {
			slices.Reverse(items)
			c.Before, c.NextBeforeSeq, c.HasOlder, c.BeforeScanLimited = items, cursor, hasMore, limited
		}
	}
	out.HasMore = c.HasOlder || c.HasNewer
	v, err := p.row(anchor)
	if err != nil {
		return out, err
	}
	c.Anchor = p.item(anchor, v, 0, logQueryExcerptChars)
	out.ScanLimited = c.BeforeScanLimited || c.AfterScanLimited
	return out, nil
}
