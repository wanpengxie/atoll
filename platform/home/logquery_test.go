package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type logFixture struct {
	t     *testing.T
	store *runtime.ChannelStores
	n     int
}

func newLogFixture(t *testing.T) *logFixture {
	t.Helper()
	store, err := runtime.OpenChannel(context.Background(), channel.ID("log-test"), filepath.Join(t.TempDir(), "log.sqlite"), runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &logFixture{t: t, store: store}
}

func (f *logFixture) add(kind message.Kind, typ, text string, parent message.ID, terminal bool, visibility message.Visibility) storespec.StoredRow {
	return f.addAs(kind, typ, text, parent, terminal, visibility, "")
}

func (f *logFixture) addAs(kind message.Kind, typ, text string, parent message.ID, terminal bool, visibility message.Visibility, who actor.ActorID) storespec.StoredRow {
	f.t.Helper()
	f.n++
	k, sender := actor.KindHuman, actor.ActorID("human:root:1")
	if kind == message.KindResponse {
		k, sender = actor.KindAgent, "agent:codex:1"
	}
	if who != "" {
		sender = who
		k, _ = actor.ParseKind(strings.SplitN(string(who), ":", 2)[0])
	}
	payload, _ := json.Marshal(map[string]string{"text": text})
	e := message.Envelope{
		ID: message.ID(fmt.Sprintf("m%d", f.n)), TS: int64(f.n * 100), TSReceived: int64(f.n * 100),
		ChannelID: "log-test", Kind: kind, Type: typ, Payload: payload, ParentID: parent,
		Sender: message.Sender{ID: sender, Kind: k}, Visibility: visibility, Audience: message.Audience{"agent:codex:1"},
	}
	result, err := f.store.Log.Append(context.Background(), &e, terminal, storespec.AppendMetadata{})
	if err != nil {
		f.t.Fatal(err)
	}
	return storespec.StoredRow{Seq: int64(result.Seq), Envelope: e, IsTerminal: terminal}
}

func (f *logFixture) query(q channelspec.LogQueryRequest) channelspec.LogQueryResponse {
	f.t.Helper()
	out, err := queryLog(context.Background(), f.store.Exchanges, q)
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func TestLogQueryFindsAnswerAndPairsInterleavedRequests(t *testing.T) {
	f := newLogFixture(t)
	first := f.add(message.KindRequest, "agent.ask", "问题一", "", false, message.VisibilityPublic)
	second := f.add(message.KindRequest, "agent.ask", "问题二", "", false, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.ask", "obsolete actor channel progress", first.Envelope.ID, false, message.VisibilityPublic)
	answer2 := f.add(message.KindResponse, "agent.ask", "ACTOR CHANNEL 的方案二", second.Envelope.ID, true, message.VisibilityPublic)
	answer1 := f.add(message.KindResponse, "agent.ask", "actor channel 的方案一", first.Envelope.ID, true, message.VisibilityPublic)

	q := channelspec.LogQueryRequest{Text: "actor channel", SenderKind: "agent", MessageType: "agent.ask", Limit: 1}
	one := f.query(q)
	if len(one.Turns) != 1 || one.Turns[0].Seq != second.Seq || len(one.Turns[0].Messages) != 2 || one.Turns[0].MatchedSeqs[0] != answer2.Seq || !one.HasMore {
		t.Fatalf("first page: %+v", one)
	}
	q.BeforeSeq, q.HeadSeq = one.NextBeforeSeq, one.HeadSeq
	two := f.query(q)
	if len(two.Turns) != 1 || two.Turns[0].Seq != first.Seq || two.Turns[0].Messages[1].Seq != answer1.Seq || two.HasMore {
		t.Fatalf("second page: %+v", two)
	}
	// Sender and text must match one row, not different halves of an exchange.
	if out := f.query(channelspec.LogQueryRequest{Text: "方案", SenderKind: "human"}); len(out.Turns) != 0 {
		t.Fatalf("cross-row AND matched: %+v", out)
	}
	if out := f.query(channelspec.LogQueryRequest{Text: "obsolete"}); len(out.Turns) != 0 {
		t.Fatalf("obsolete progress matched: %+v", out)
	}
}

func TestLogQuerySnapshotPaginationAndLatestOpenResponse(t *testing.T) {
	f := newLogFixture(t)
	old := f.add(message.KindRequest, "agent.ask", "older", "", false, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.ask", "old transient", old.Envelope.ID, false, message.VisibilityPublic)
	latest := f.add(message.KindResponse, "agent.ask", "needle progress", old.Envelope.ID, false, message.VisibilityPublic)
	f.add(message.KindEvent, "note", "needle newest", "", false, message.VisibilityPublic)
	one := f.query(channelspec.LogQueryRequest{Text: "needle", Limit: 1})
	f.add(message.KindResponse, "agent.ask", "new terminal", old.Envelope.ID, true, message.VisibilityPublic)
	f.add(message.KindEvent, "note", "needle appended", "", false, message.VisibilityPublic)
	two := f.query(channelspec.LogQueryRequest{Text: "needle", Limit: 1, HeadSeq: one.HeadSeq, BeforeSeq: one.NextBeforeSeq})
	if len(two.Turns) != 1 || two.Turns[0].MatchedSeqs[0] != latest.Seq || two.HeadSeq != one.HeadSeq || two.HasMore {
		t.Fatalf("snapshot drift: %+v", two)
	}
	out := f.query(channelspec.LogQueryRequest{Text: "needle"})
	if len(out.Turns) != 2 {
		t.Fatalf("fresh query should hide completed progress: %+v", out)
	}
}

func TestLogQueryVisibilityHousekeepingAndExactRead(t *testing.T) {
	f := newLogFixture(t)
	private := f.add(message.KindEvent, "note", "needle secret", "", false, message.VisibilitySystem)
	for _, typ := range []string{"system.log.query", "system.log.recent", "agent.options", "actor.describe"} {
		r := f.add(message.KindRequest, typ, "needle", "", false, message.VisibilityPublic)
		a := f.add(message.KindResponse, typ, "needle", r.Envelope.ID, true, message.VisibilityPublic)
		for _, row := range []storespec.StoredRow{r, a} {
			_, err := queryLog(context.Background(), f.store.Exchanges, channelspec.LogQueryRequest{ReadSeq: row.Seq})
			if !errors.Is(err, channelspec.ErrLogMessageNotFound) {
				t.Fatalf("housekeeping read: %v", err)
			}
		}
	}
	r := f.add(message.KindRequest, "agent.ask", "public question", "", false, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.ask", "needle public progress", r.Envelope.ID, false, message.VisibilityPublic)
	f.add(message.KindResponse, "agent.ask", "needle private terminal", r.Envelope.ID, true, message.VisibilitySystem)
	if out := f.query(channelspec.LogQueryRequest{Text: "needle"}); len(out.Turns) != 0 {
		t.Fatalf("leaked rows: %+v", out)
	}
	for _, seq := range []int64{private.Seq, 9999} {
		_, err := queryLog(context.Background(), f.store.Exchanges, channelspec.LogQueryRequest{ReadSeq: seq})
		if !errors.Is(err, channelspec.ErrLogMessageNotFound) {
			t.Fatalf("read %d: %v", seq, err)
		}
	}
}

func TestLogQueryScanBudgetCanContinueAcrossEmptyPage(t *testing.T) {
	f := newLogFixture(t)
	hit := f.add(message.KindEvent, "note", "needle", "", false, message.VisibilityPublic)
	for range logQueryScanLimit + 1 {
		f.add(message.KindEvent, "note", "unrelated", "", false, message.VisibilityPublic)
	}
	one := f.query(channelspec.LogQueryRequest{Text: "needle"})
	if len(one.Turns) != 0 || !one.HasMore || !one.ScanLimited || one.Scanned != logQueryScanLimit {
		t.Fatalf("scan boundary: %+v", one)
	}
	two := f.query(channelspec.LogQueryRequest{Text: "needle", BeforeSeq: one.NextBeforeSeq, HeadSeq: one.HeadSeq})
	if len(two.Turns) != 1 || two.Turns[0].Seq != hit.Seq || two.HasMore || two.ScanLimited {
		t.Fatalf("continuation: %+v", two)
	}
}

func TestLogQueryUnicodeExcerptAndLosslessChunkRead(t *testing.T) {
	f := newLogFixture(t)
	r := f.add(message.KindEvent, "note", strings.Repeat("甲", 8500)+"关键词"+strings.Repeat("乙", 2000), "", false, message.VisibilityPublic)
	out := f.query(channelspec.LogQueryRequest{Text: "关键词"})
	m := out.Turns[0].Messages[0]
	if !m.Truncated || m.Offset == 0 || utf8.RuneCountInString(m.PayloadText) > logQueryExcerptChars || !strings.Contains(m.PayloadText, "关键词") {
		t.Fatalf("excerpt: %+v", m)
	}
	var got strings.Builder
	for offset := 0; ; {
		chunk := f.query(channelspec.LogQueryRequest{ReadSeq: r.Seq, Offset: offset, View: "raw"}).Message
		if chunk.Offset != offset {
			t.Fatalf("offset: %+v", chunk)
		}
		got.WriteString(chunk.PayloadText)
		if chunk.NextOffset == nil {
			break
		}
		offset = *chunk.NextOffset
	}
	if got.String() != logPayloadText(r) {
		t.Fatal("chunk reassembly changed original payload")
	}
	if chunk := f.query(channelspec.LogQueryRequest{ReadSeq: r.Seq, Offset: 999999}).Message; chunk.PayloadText != "" || chunk.NextOffset != nil {
		t.Fatalf("past EOF: %+v", chunk)
	}
	// Canonical normalization preserves large numbers and decodes Unicode escapes.
	row := storespec.StoredRow{Envelope: message.Envelope{Payload: []byte(`{"text":"\u65b9\u6848","n":9007199254740993}`)}}
	if text := logPayloadText(row); !strings.Contains(text, "方案") || !strings.Contains(text, "9007199254740993") {
		t.Fatal(text)
	}
}

func TestLogQueryTimeBoundsAndNestedRequest(t *testing.T) {
	f := newLogFixture(t)
	r := f.add(message.KindRequest, "agent.ask", "root", "", false, message.VisibilityPublic)
	n := f.add(message.KindRequest, "tool.run", "nested", r.Envelope.ID, false, message.VisibilityPublic)
	a := f.add(message.KindResponse, "tool.run", "answer", n.Envelope.ID, true, message.VisibilityPublic)
	out := f.query(channelspec.LogQueryRequest{MessageType: "tool.run", Sender: "agent:codex:1", FromTS: a.Envelope.TSReceived, ToTS: a.Envelope.TSReceived + 1})
	if len(out.Turns) != 1 || out.Turns[0].Seq != n.Seq || out.Turns[0].MatchedSeqs[0] != a.Seq || out.Turns[0].Messages[0].ParentID != r.Envelope.ID {
		t.Fatalf("nested/time/sender: %+v", out)
	}
	if out := f.query(channelspec.LogQueryRequest{Text: "answer", ToTS: a.Envelope.TSReceived}); len(out.Turns) != 0 {
		t.Fatal("to_ts must be exclusive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryLog(ctx, f.store.Exchanges, channelspec.LogQueryRequest{Text: "answer"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
