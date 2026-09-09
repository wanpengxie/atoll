package home

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (f *logFixture) payload(kind message.Kind, typ string, value any, parent message.ID, terminal bool, visibility message.Visibility) storespec.StoredRow {
	f.t.Helper()
	// Use the regular fixture identity/clock, then append a distinct envelope
	// with the desired protocol payload. The scaffold is hidden from queries.
	row := f.add(kind, typ, "scaffold", "", false, message.VisibilitySystem)
	row.Envelope.ID += "-payload"
	row.Envelope.ParentID = parent
	row.IsTerminal = terminal
	row.Envelope.Visibility = visibility
	row.Envelope.Payload = canonicalTestPayload(value)
	result, err := f.store.Log.Append(context.Background(), &row.Envelope, terminal, storespec.AppendMetadata{})
	if err != nil {
		f.t.Fatal(err)
	}
	row.Seq = int64(result.Seq)
	return row
}

func TestLogConversationTextAndRawMetadata(t *testing.T) {
	f := newLogFixture(t)
	q := f.payload(message.KindRequest, "agent.ask", map[string]any{"body": map[string]any{"text": "RSI question", "origin": map[string]string{"label": "browser-marker"}}}, "", false, message.VisibilityPublic)
	a := f.payload(message.KindResponse, "agent.ask", map[string]any{"text": "RSI answer", "usage": map[string]string{"model": "metadata-model"}}, q.Envelope.ID, true, message.VisibilityPublic)
	for _, needle := range []string{"metadata-model", "browser-marker"} {
		if out := f.query(channelspec.LogQueryRequest{Text: needle}); len(out.Turns) != 0 {
			t.Fatalf("metadata matched: %+v", out)
		}
		if out := f.query(channelspec.LogQueryRequest{Text: needle, View: "raw"}); len(out.Turns) != 1 {
			t.Fatalf("raw miss: %+v", out)
		}
	}
	read := f.query(channelspec.LogQueryRequest{ReadID: string(a.Envelope.ID)})
	if read.Message.PayloadText != "RSI answer" || read.Message.ContentSource != "text" || read.Message.State != "answered" {
		t.Fatalf("read: %+v", read.Message)
	}
	// Tools without a known text field remain queryable as labeled structures.
	tool := f.payload(message.KindRequest, "tool.run", map[string]string{"path": "artifact-special"}, "", false, message.VisibilityPublic)
	out := f.query(channelspec.LogQueryRequest{Text: "artifact-special"})
	if len(out.Turns) != 1 || out.Turns[0].Seq != tool.Seq || out.Turns[0].Messages[0].ContentSource != "structured_json" {
		t.Fatal(out)
	}
}

func TestLogReplacementAcceptanceAndSnapshot(t *testing.T) {
	f := newLogFixture(t)
	old := f.add(message.KindRequest, "agent.ask", "obsolete-wording", "", false, message.VisibilityPublic)
	edit := f.payload(message.KindRequest, "agent.replace", map[string]any{"body": map[string]string{"target": string(old.Envelope.ID), "old_text": "obsolete-wording", "new_text": "effective-wording"}}, "", false, message.VisibilityPublic)
	head := edit.Seq
	f.payload(message.KindResponse, "agent.ask", map[string]string{"status": "completed", "replaced_by": string(edit.Envelope.ID)}, old.Envelope.ID, true, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.replace", "implemented", edit.Envelope.ID, true, message.VisibilityPublic)
	if out := f.query(channelspec.LogQueryRequest{Text: "obsolete-wording"}); len(out.Turns) != 0 {
		t.Fatalf("old body still searchable: %+v", out)
	}
	if out := f.query(channelspec.LogQueryRequest{Text: "obsolete-wording", HeadSeq: head}); len(out.Turns) != 1 || out.Turns[0].Seq != old.Seq {
		t.Fatalf("unconfirmed edit superseded original: %+v", out)
	}
	if out := f.query(channelspec.LogQueryRequest{Text: "effective-wording"}); len(out.Turns) != 1 || out.Turns[0].Seq != edit.Seq || out.Turns[0].Messages[0].ContentSource != "replacement_text" {
		t.Fatalf("edit: %+v", out)
	}
	read := f.query(channelspec.LogQueryRequest{ReadSeq: old.Seq})
	if read.Message.State != "replaced" || read.Message.PayloadText != "" || read.Message.ContentSource != "superseded" {
		t.Fatal(read.Message)
	}
	if raw := f.query(channelspec.LogQueryRequest{ReadSeq: old.Seq, View: "raw"}); !strings.Contains(raw.Message.PayloadText, "obsolete-wording") {
		t.Fatal(raw)
	}
	related := f.query(channelspec.LogQueryRequest{RelatedTo: string(old.Envelope.ID)})
	found := false
	for _, r := range related.Related {
		if r.Relation.Kind == "replaced_by" && r.Message != nil && r.Message.ID == edit.Envelope.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("cannot follow replacement: %+v", related)
	}
}

func TestLogStatesStatisticsAndEmptyNeighbors(t *testing.T) {
	f := newLogFixture(t)
	anchor := f.add(message.KindRequest, "agent.ask", "question", "", false, message.VisibilityPublic)
	f.payload(message.KindResponse, "agent.ask", map[string]string{"status": "completed", "merged_into": "missing"}, anchor.Envelope.ID, true, message.VisibilityPublic)
	f.add(message.KindEvent, "terminal.session", "closed", "", false, message.VisibilityPublic)
	note := f.addAs(message.KindEvent, "chat.note", "another role speaks", "", false, message.VisibilityPublic, "tool:reporter:1")
	out := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 1})
	if len(out.Context.After) != 1 || out.Context.After[0].Seq != note.Seq || out.Context.Anchor.State != "merged" {
		t.Fatalf("noisy neighbors: %+v", out.Context)
	}
	raw := f.query(channelspec.LogQueryRequest{AroundSeq: anchor.Seq, Radius: 3, View: "raw"})
	if len(raw.Context.After) != 3 {
		t.Fatalf("raw lost operations: %+v", raw.Context)
	}
	s := f.query(channelspec.LogQueryRequest{GroupBy: "sender_kind"}).Stats
	if s.Questions != 1 || s.Answers != 0 || s.States["merged"] != 1 || s.MatchedMessages != 3 {
		t.Fatalf("status packet counted as answer: %+v", s)
	}
}

func TestLogRelationSnapshotVisibilityAndCycles(t *testing.T) {
	f := newLogFixture(t)
	q := f.add(message.KindRequest, "agent.ask", "anchor", "", false, message.VisibilityPublic)
	private := f.add(message.KindRequest, "agent.ask", "secret", "", false, message.VisibilitySystem)
	a := f.payload(message.KindResponse, "agent.ask", map[string]string{"status": "completed", "merged_into": string(private.Envelope.ID), "replaced_by": string(q.Envelope.ID), "preempted_by": "absent"}, q.Envelope.ID, true, message.VisibilityPublic)
	out := f.query(channelspec.LogQueryRequest{RelatedTo: string(a.Envelope.ID)})
	if len(out.Related) != 3 {
		t.Fatalf("must deduplicate IDs and not recurse: %+v", out.Related)
	}
	for _, r := range out.Related {
		if r.Relation.MessageID != string(q.Envelope.ID) && (!r.Unavailable || r.Message != nil) {
			t.Fatal(r)
		}
	}
	_, err := queryLog(context.Background(), f.store.Exchanges, channelspec.LogQueryRequest{ReadID: string(a.Envelope.ID), HeadSeq: q.Seq})
	if !errors.Is(err, channelspec.ErrLogMessageNotFound) {
		t.Fatal(err)
	}
}

func TestLogReturnedActionsAreValidAndChunksKeepSnapshot(t *testing.T) {
	f := newLogFixture(t)
	text := strings.Repeat("正文", 6000)
	row := f.add(message.KindRequest, "agent.ask", text, "", false, message.VisibilityPublic)
	out := f.query(channelspec.LogQueryRequest{Text: "正文", Limit: 1})
	check := func(out channelspec.LogQueryResponse) {
		t.Helper()
		if out.View != "conversation" || len(out.Guidance) == 0 {
			t.Fatal("missing usage guidance")
		}
		for _, a := range out.Next {
			if err := a.Request.Validate(); err != nil {
				t.Fatalf("invalid next action %+v: %v", a, err)
			}
		}
	}
	check(out)
	q := channelspec.LogQueryRequest{ReadID: string(row.Envelope.ID), HeadSeq: out.HeadSeq}
	var got strings.Builder
	for {
		part := f.query(q)
		check(part)
		got.WriteString(part.Message.PayloadText)
		if part.Message.NextRead == nil {
			break
		}
		q = *part.Message.NextRead
		if q.View != "conversation" || q.HeadSeq != out.HeadSeq || q.Offset == 0 {
			t.Fatal(q)
		}
		if err := q.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != text {
		t.Fatal("plain-text chunks changed content")
	}
	check(f.query(channelspec.LogQueryRequest{AroundSeq: row.Seq}))
	check(f.query(channelspec.LogQueryRequest{GroupBy: "sender"}))
}
