package home

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type logProjection struct {
	ctx    context.Context
	reader storespec.VisibleExchangeQuery
	head   int64
	view   string
	cache  map[message.ID]projectedLogRow
}
type projectedLogRow struct {
	text, source, state string
	include             bool
	links               []channelspec.LogQueryRelation
}

func logFields(row storespec.StoredRow) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	_, body, err := harness.UnwrapPayload(row.Envelope.Payload)
	if err == nil {
		_ = json.Unmarshal(body, &fields)
	}
	return fields
}
func logString(fields map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(fields[key], &value)
	return value
}
func logBody(fields map[string]json.RawMessage) map[string]json.RawMessage {
	return fields
}
func logResponseState(row storespec.StoredRow) string {
	f := logFields(row)
	if !row.IsTerminal {
		return "processing"
	}
	for _, v := range []struct{ field, state string }{{"replaced_by", "replaced"}, {"merged_into", "merged"}, {"preempted_by", "interrupted"}} {
		if logString(f, v.field) != "" {
			return v.state
		}
	}
	if status := logString(f, "status"); status == "failed" || status == "cancelled" {
		return "failed"
	}
	if logString(f, "text") != "" {
		return "answered"
	}
	return "completed_without_answer"
}
func logLinks(row storespec.StoredRow) []channelspec.LogQueryRelation {
	var out []channelspec.LogQueryRelation
	if row.Envelope.ParentID != "" {
		out = append(out, channelspec.LogQueryRelation{Kind: "reply_to", MessageID: string(row.Envelope.ParentID)})
	}
	// These keys are interpreted only in the Agent protocol, not arbitrary tool
	// payloads or quoted prose. A reference does not grant access to its target.
	if !strings.HasPrefix(row.Envelope.Type, "agent.") {
		return out
	}
	f := logFields(row)
	if row.Envelope.Kind == message.KindResponse {
		for _, key := range []string{"replaced_by", "merged_into", "preempted_by"} {
			if id := logString(f, key); id != "" {
				out = append(out, channelspec.LogQueryRelation{Kind: key, MessageID: id})
			}
		}
	} else if row.Envelope.Type == "agent.replace" {
		if id := logString(logBody(f), "target"); id != "" {
			out = append(out, channelspec.LogQueryRelation{Kind: "edits", MessageID: id})
		}
	}
	return out
}

func (p *logProjection) row(row storespec.StoredRow) (projectedLogRow, error) {
	if v, ok := p.cache[row.Envelope.ID]; ok {
		return v, nil
	}
	e := row.Envelope
	// Conversation hides control/runtime plumbing. raw is the diagnostic and
	// recovery view, so public housekeeping rows remain inspectable there.
	v := projectedLogRow{include: p.view == "raw" || !channelspec.HousekeepingWord(e.Type), source: "raw_json", links: logLinks(row)}
	f := logFields(row)
	if e.Kind == message.KindRequest {
		v.state = "processing"
		reply, found, err := p.reader.ReadVisibleReply(p.ctx, string(e.ID), p.head)
		if err != nil {
			return v, err
		}
		if found {
			v.state = logResponseState(reply)
			v.links = append(v.links, channelspec.LogQueryRelation{Kind: "response", MessageID: string(reply.Envelope.ID)})
			for _, link := range logLinks(reply) {
				if link.Kind != "reply_to" {
					v.links = append(v.links, link)
				}
			}
		}
	} else if e.Kind == message.KindResponse {
		v.state = logResponseState(row)
	}
	if p.view == "raw" {
		v.text = logPayloadText(row)
	} else {
		v.source = "text"
		switch {
		case strings.HasPrefix(e.Type, "terminal.") || strings.HasPrefix(e.Type, "ui."):
			v.include = false
		case e.Type == "agent.replace" && e.Kind == message.KindRequest:
			v.text = logString(logBody(f), "new_text")
			v.source = "replacement_text"
		case e.Type == "agent.ask" && e.Kind == message.KindRequest:
			v.text = logString(logBody(f), "text")
		case strings.HasPrefix(e.Type, "agent.") && e.Kind == message.KindResponse:
			v.text = logString(f, "text")
		case logString(logBody(f), "text") != "":
			v.text = logString(logBody(f), "text")
		default:
			v.text = logPayloadText(row)
			v.source = "structured_json"
		}
		// The original request's own terminal is authoritative evidence that it
		// was replaced. Never guess acceptance from an attempted edit alone.
		if e.Kind == message.KindRequest && v.state == "replaced" {
			v.text = ""
			v.source = "superseded"
		}
	}
	p.cache[e.ID] = v
	return v, nil
}

func (p *logProjection) item(row storespec.StoredRow, v projectedLogRow, offset, limit int) channelspec.LogQueryMessage {
	m := logQueryMessage(row, v.text, offset, limit)
	m.ContentSource, m.State, m.Relations = v.source, v.state, v.links
	if m.NextOffset != nil {
		m.NextRead = &channelspec.LogQueryRequest{ReadSeq: m.Seq, Offset: *m.NextOffset, View: p.view, HeadSeq: p.head}
	}
	return m
}

func queryLogRead(ctx context.Context, reader storespec.VisibleExchangeQuery, q channelspec.LogQueryRequest) (channelspec.LogQueryResponse, error) {
	out := channelspec.LogQueryResponse{Turns: []channelspec.LogQueryTurn{}}
	page, err := reader.ReadVisibleExchanges(ctx, 0, q.HeadSeq, 1)
	if err != nil {
		return out, err
	}
	out.HeadSeq = page.HeadSeq
	p := &logProjection{ctx: ctx, reader: reader, head: out.HeadSeq, view: q.View, cache: map[message.ID]projectedLogRow{}}
	id := q.ReadID
	if q.RelatedTo != "" {
		id = q.RelatedTo
	}
	var row storespec.StoredRow
	var found bool
	if id != "" {
		row, found, err = reader.FindVisibleID(ctx, id)
	} else {
		row, found, err = reader.FindVisibleSeq(ctx, q.ReadSeq)
	}
	if err != nil {
		return out, err
	}
	if !found || row.Seq > out.HeadSeq || (q.View != "raw" && channelspec.HousekeepingWord(row.Envelope.Type)) {
		return out, channelspec.ErrLogMessageNotFound
	}
	v, err := p.row(row)
	if err != nil {
		return out, err
	}
	// Explicit reads remain available for operation records; view controls the
	// content representation, while inclusion only filters search/neighbors.
	m := p.item(row, v, q.Offset, logQueryReadChars)
	out.Message = &m
	if q.RelatedTo != "" {
		// One hop per request, deduplicated; cycles never trigger recursion.
		seen := map[string]bool{string(row.Envelope.ID): true}
		for _, link := range v.links {
			r := channelspec.LogQueryRelated{Relation: link}
			if !seen[link.MessageID] {
				seen[link.MessageID] = true
				target, ok, err := reader.FindVisibleID(ctx, link.MessageID)
				if err != nil {
					return out, err
				}
				if ok && target.Seq <= out.HeadSeq && (q.View == "raw" || !channelspec.HousekeepingWord(target.Envelope.Type)) {
					pv, err := p.row(target)
					if err != nil {
						return out, err
					}
					item := p.item(target, pv, 0, logQueryExcerptChars)
					r.Message = &item
				} else {
					r.Unavailable = true
				}
			} else {
				continue
			}
			out.Related = append(out.Related, r)
		}
	}
	return out, nil
}
