package home

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestLogContextVisibilityAndSnapshotResponses(t *testing.T) {
	f := newLogFixture(t)
	anchor := f.add(message.KindEvent, "chat.note", "anchor", "", false, message.VisibilityPublic)
	req := f.add(message.KindRequest, "agent.ask", "question", "", false, message.VisibilityPublic)
	progress := f.add(message.KindResponse, "agent.ask", "progress", req.Envelope.ID, false, message.VisibilityPublic)
	one := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 10})
	f.add(message.KindResponse, "agent.ask", "private final", req.Envelope.ID, true, message.VisibilitySystem)
	two := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 10, HeadSeq: one.HeadSeq})
	if len(two.Context.After) != 2 || two.Context.After[1].Seq != progress.Seq {
		t.Fatalf("snapshot lost provisional: %+v", two.Context)
	}
	three := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 10})
	if len(three.Context.After) != 1 || three.Context.After[0].Seq != req.Seq {
		t.Fatalf("private terminal resurrected progress: %+v", three.Context)
	}
	private := f.add(message.KindRequest, "agent.ask", "private request", "", false, message.VisibilitySystem)
	f.add(message.KindResponse, "agent.ask", "public child of private request", private.Envelope.ID, true, message.VisibilityPublic)
	control := f.add(message.KindRequest, "system.log.query", "control", "", false, message.VisibilityPublic)
	for _, seq := range []int64{private.Seq, control.Seq, 99999} {
		_, err := queryLog(context.Background(), f.store.Exchanges, channelspec.LogQueryRequest{AroundSeq: seq})
		if !errors.Is(err, channelspec.ErrLogMessageNotFound) {
			t.Fatalf("unreadable anchor %d: %v", seq, err)
		}
	}
	if out := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 10}); len(out.Context.After) != 1 {
		t.Fatalf("hidden parent leak: %+v", out.Context)
	}
}

func TestLogStatsSharedRolesProjectionAndParticipant(t *testing.T) {
	f := newLogFixture(t)
	q := f.add(message.KindRequest, "agent.ask", "design?", "", false, message.VisibilityPublic)
	f.addAs(message.KindResponse, "agent.ask", "obsolete", q.Envelope.ID, false, message.VisibilityPublic, "agent:claude:1")
	f.addAs(message.KindResponse, "agent.ask", "proposal", q.Envelope.ID, true, message.VisibilityPublic, "agent:claude:1")
	f.addAs(message.KindEvent, "chat.note", "objection", "", false, message.VisibilityPublic, "agent:codex:1")
	f.add(message.KindRequest, "system.log.query", "excluded", "", false, message.VisibilityPublic)
	f.add(message.KindEvent, "secret", "excluded", "", false, message.VisibilitySystem)
	out := f.query(channelspec.LogQueryRequest{GroupBy: "sender"})
	if out.Stats == nil || out.Stats.Scope != "scanned_page" || out.Stats.MatchedMessages != 3 || out.Stats.MatchedExchanges != 2 || len(out.Stats.Buckets) != 3 || len(out.Turns) != 0 || out.HasMore {
		t.Fatalf("shared roles: %+v stats=%+v", out, out.Stats)
	}
	for _, b := range out.Stats.Buckets {
		if b.Messages != 1 || b.FirstSeq != b.LastSeq || b.FromTS != b.ThroughTS {
			t.Fatalf("bucket: %+v", b)
		}
	}
	participant := f.query(channelspec.LogQueryRequest{Participant: "agent:claude:1", GroupBy: "sender_kind"})
	if participant.Stats.MatchedMessages != 1 || participant.Stats.Buckets[0].Key != "agent" {
		t.Fatalf("participant: %+v", participant.Stats)
	}
	// Explicit audience counts as participation; all three rows address Codex.
	if s := f.query(channelspec.LogQueryRequest{Participant: "agent:codex:1", GroupBy: "day"}).Stats; s.MatchedMessages != 3 || len(s.Buckets) != 1 || s.Buckets[0].Key != "1970-01-01" {
		t.Fatalf("day: %+v", s)
	}
	if s := f.query(channelspec.LogQueryRequest{Text: "proposal", SenderKind: "human", GroupBy: "message_type"}).Stats; s.MatchedMessages != 0 {
		t.Fatalf("same-row filters: %+v", s)
	}
}

func TestLogStatsPaginationIsPartialAndSnapshotStable(t *testing.T) {
	f := newLogFixture(t)
	for range logQueryScanLimit + 1 {
		f.add(message.KindEvent, "chat.note", "count", "", false, message.VisibilityPublic)
	}
	one := f.query(channelspec.LogQueryRequest{GroupBy: "message_type"})
	if !one.HasMore || !one.ScanLimited || one.Stats.MatchedMessages != logQueryScanLimit {
		t.Fatalf("first: %+v", one)
	}
	f.add(message.KindEvent, "chat.note", "new outside snapshot", "", false, message.VisibilityPublic)
	two := f.query(channelspec.LogQueryRequest{GroupBy: "message_type", HeadSeq: one.HeadSeq, BeforeSeq: one.NextBeforeSeq})
	if two.HasMore || two.Stats.MatchedMessages != 1 || two.HeadSeq != one.HeadSeq {
		t.Fatalf("second: %+v stats=%+v", two, two.Stats)
	}
}

func TestLogContextCrossRoleChronologyAndPaging(t *testing.T) {
	f := newLogFixture(t)
	question := f.add(message.KindRequest, "agent.ask", "question", "", false, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.ask", "obsolete", question.Envelope.ID, false, message.VisibilityPublic)
	other := f.addAs(message.KindEvent, "chat.note", "different discussion", "", false, message.VisibilityPublic, "agent:claude:1")
	answer := f.add(message.KindResponse, "agent.ask", "answer", question.Envelope.ID, true, message.VisibilityPublic)
	f.add(message.KindRequest, "system.log.query", "housekeeping", "", false, message.VisibilityPublic)
	f.add(message.KindEvent, "chat.note", "hidden", "", false, message.VisibilitySystem)
	objection := f.addAs(message.KindEvent, "chat.note", "objection", "", false, message.VisibilityPublic, "agent:claude:1")
	last := f.add(message.KindEvent, "chat.note", "conclusion", "", false, message.VisibilityPublic)
	one := f.query(channelspec.LogQueryRequest{AroundSeq: answer.Seq, Radius: 1})
	c := one.Context
	if c == nil || c.Anchor.Seq != answer.Seq || c.Anchor.ParentID != question.Envelope.ID || len(c.Before) != 1 || c.Before[0].Seq != other.Seq || len(c.After) != 1 || c.After[0].Seq != objection.Seq || !c.HasOlder || !c.HasNewer {
		t.Fatalf("context: %+v", c)
	}
	f.add(message.KindEvent, "chat.note", "outside snapshot", "", false, message.VisibilityPublic)
	two := f.query(channelspec.LogQueryRequest{AroundSeq: answer.Seq, Radius: 10, HeadSeq: one.HeadSeq, BeforeSeq: c.NextBeforeSeq, AfterSeq: c.NextAfterSeq})
	c = two.Context
	if len(c.Before) != 1 || c.Before[0].Seq != question.Seq || len(c.After) != 1 || c.After[0].Seq != last.Seq || two.HasMore || two.HeadSeq != one.HeadSeq {
		t.Fatalf("continuation: %+v", c)
	}
}

func TestLogContextEmptyScanLimitedCanContinue(t *testing.T) {
	f := newLogFixture(t)
	older := f.add(message.KindEvent, "chat.note", "older", "", false, message.VisibilityPublic)
	for range logQueryScanLimit {
		f.add(message.KindRequest, "system.log.query", "hidden control", "", false, message.VisibilityPublic)
	}
	anchor := f.add(message.KindEvent, "chat.note", "anchor", "", false, message.VisibilityPublic)
	for range logQueryScanLimit {
		f.add(message.KindRequest, "system.log.query", "hidden control", "", false, message.VisibilityPublic)
	}
	newer := f.add(message.KindEvent, "chat.note", "newer", "", false, message.VisibilityPublic)
	one := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 1})
	c := one.Context
	if len(c.Before) != 0 || len(c.After) != 0 || !c.BeforeScanLimited || !c.AfterScanLimited || !one.HasMore {
		t.Fatalf("empty page: %+v", c)
	}
	two := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 1, HeadSeq: one.HeadSeq, BeforeSeq: c.NextBeforeSeq, AfterSeq: c.NextAfterSeq})
	c = two.Context
	if len(c.Before) != 1 || c.Before[0].Seq != older.Seq || len(c.After) != 1 || c.After[0].Seq != newer.Seq || two.HasMore {
		t.Fatalf("continued: %+v", c)
	}
}
