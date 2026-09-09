package home

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	logQueryBatch        = 16
	logQueryScanLimit    = 512
	logQueryScanBytes    = 8 << 20
	logQueryExcerptChars = 1500
	logQueryReadChars    = 8000
)

func queryLogPage(ctx context.Context, reader storespec.VisibleExchangeQuery, q channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
	out := channelspec.LogQueryResponse{Turns: []channelspec.LogQueryTurn{}}
	if err := q.Validate(); err != nil {
		return out, err
	}
	if q.AroundSeq > 0 {
		return queryLogContext(ctx, reader, q)
	}
	if q.ReadSeq > 0 || q.ReadID != "" || q.RelatedTo != "" {
		return queryLogRead(ctx, reader, q)
	}
	limit := q.Limit
	if limit == 0 {
		limit = 5
	}
	before, head := q.BeforeSeq, q.HeadSeq
	needle := strings.ToLower(strings.TrimSpace(q.Text))
	if q.GroupBy != "" {
		out.Stats = &channelspec.LogQueryStats{Scope: "scanned_page", GroupBy: q.GroupBy, Buckets: []channelspec.LogQueryBucket{}, States: map[string]int{}}
	}
	buckets := map[string]int{}
	scannedBytes := 0
	p := &logProjection{ctx: ctx, reader: reader, view: q.View, cache: map[message.ID]projectedLogRow{}}
	for {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		page, err := reader.ReadVisibleExchanges(ctx, before, head, logQueryBatch)
		if err != nil {
			return out, err
		}
		head = page.HeadSeq
		out.HeadSeq = head
		p.head = head
		for index, rows := range page.Exchanges {
			start := rows[0]
			out.Scanned++
			before = start.Seq
			out.HasMore = index+1 < len(page.Exchanges) || page.HasOlder
			out.NextBeforeSeq = before
			texts := make([]string, len(rows))
			projected := make([]projectedLogRow, len(rows))
			matched := []int64{}
			for i, row := range rows {
				scannedBytes += len(row.Envelope.Payload)
				if q.View != "raw" && (channelspec.HousekeepingWord(start.Envelope.Type) || channelspec.HousekeepingWord(row.Envelope.Type)) {
					continue
				}
				v, err := p.row(row)
				if err != nil {
					return out, err
				}
				projected[i] = v
				if !v.include {
					continue
				}
				texts[i] = v.text
				if logQueryMatches(row, texts[i], needle, q) {
					matched = append(matched, row.Seq)
					if out.Stats != nil {
						addLogStat(out.Stats, buckets, row)
						if row.Envelope.Kind == message.KindRequest {
							out.Stats.Questions++
						}
						if row.Envelope.Kind == message.KindResponse && logString(logFields(row), "text") != "" {
							out.Stats.Answers++
						}
					}
				}
			}
			if len(matched) > 0 && out.Stats != nil {
				out.Stats.MatchedExchanges++
				if state := projected[0].state; state != "" {
					out.Stats.States[state]++
				}
			}
			if len(matched) > 0 && out.Stats == nil {
				turn := channelspec.LogQueryTurn{Seq: start.Seq, State: projected[0].state, MatchedSeqs: matched, Messages: []channelspec.LogQueryMessage{}}
				for i, row := range rows {
					if !projected[i].include {
						continue
					}
					offset := 0
					if needle != "" {
						if pos := strings.Index(strings.ToLower(texts[i]), needle); pos >= 0 {
							// Count in the lowercased text: Unicode case mapping can
							// change byte width, while retaining character positions.
							offset = max(0, utf8.RuneCountInString(strings.ToLower(texts[i])[:pos])-200)
						}
					}
					turn.Messages = append(turn.Messages, p.item(row, projected[i], offset, logQueryExcerptChars))
				}
				out.Turns = append(out.Turns, turn)
			}
			if len(out.Turns) >= limit {
				return out, nil
			}
			if out.Scanned >= logQueryScanLimit || scannedBytes >= logQueryScanBytes {
				out.ScanLimited = out.HasMore
				return out, nil
			}
		}
		if !page.HasOlder || len(page.Exchanges) == 0 {
			out.HasMore = false
			return out, nil
		}
	}
}

// All filters must match the SAME message. The sibling request/response is
// returned for context, and is not claimed to satisfy the filters itself.
func logQueryMatches(row storespec.StoredRow, text, needle string, q channelspec.LogQueryRequest) bool {
	e := row.Envelope
	app, _, _ := harness.UnwrapPayload(e.Payload)
	return (q.Sender == "" || string(e.Sender.ID) == q.Sender) &&
		(q.Participant == "" || string(e.Sender.ID) == q.Participant || slices.Contains(e.Audience, actor.ActorID(q.Participant))) &&
		(q.SenderKind == "" || string(e.Sender.Kind) == q.SenderKind) &&
		(q.MessageType == "" || e.Type == q.MessageType) &&
		(q.SessionID == "" || app.Session == q.SessionID) &&
		(q.FromTS == 0 || e.TSReceived >= q.FromTS) &&
		(q.ToTS == 0 || e.TSReceived < q.ToTS) &&
		(needle == "" || strings.Contains(strings.ToLower(text), needle))
}

func addLogStat(stats *channelspec.LogQueryStats, indices map[string]int, row storespec.StoredRow) {
	e := row.Envelope
	key := string(e.Sender.ID)
	switch stats.GroupBy {
	case "sender_kind":
		key = string(e.Sender.Kind)
	case "message_type":
		key = e.Type
	case "day":
		key = time.UnixMilli(e.TSReceived).UTC().Format("2006-01-02")
	}
	index, found := indices[key]
	if !found {
		index = len(stats.Buckets)
		indices[key] = index
		stats.Buckets = append(stats.Buckets, channelspec.LogQueryBucket{Key: key, FirstSeq: row.Seq, LastSeq: row.Seq, FromTS: e.TSReceived, ThroughTS: e.TSReceived})
	}
	b := &stats.Buckets[index]
	b.Messages++
	b.FirstSeq = min(b.FirstSeq, row.Seq)
	b.LastSeq = max(b.LastSeq, row.Seq)
	b.FromTS = min(b.FromTS, e.TSReceived)
	b.ThroughTS = max(b.ThroughTS, e.TSReceived)
	stats.MatchedMessages++
}

// Normalize Unicode escapes so Chinese text is searchable. Quotes and control
// characters retain JSON escaping. UseNumber preserves numeric precision.
// Normalized JSON is deterministic; offsets survive independent read calls.
func logPayloadText(row storespec.StoredRow) string {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(row.Envelope.Payload))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return string(row.Envelope.Payload)
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(value) != nil {
		return string(row.Envelope.Payload)
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

func logQueryMessage(row storespec.StoredRow, text string, offset, limit int) channelspec.LogQueryMessage {
	runes := []rune(text)
	offset = min(offset, len(runes))
	end := min(len(runes), offset+limit)
	e := row.Envelope
	out := channelspec.LogQueryMessage{
		Seq: row.Seq, ID: e.ID, Sender: e.Sender, Audience: e.Audience,
		Kind: e.Kind, MessageType: e.Type, ParentID: e.ParentID, TSReceived: e.TSReceived,
		Terminal: row.IsTerminal, PayloadText: string(runes[offset:end]),
		TotalChars: len(runes), Offset: offset, Truncated: offset > 0 || end < len(runes),
	}
	if end < len(runes) {
		out.NextOffset = &end
	}
	return out
}
