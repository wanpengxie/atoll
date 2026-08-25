package home

import (
	"context"
	"fmt"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type historyQueryStub struct {
	rows  []storespec.StoredRow
	calls int
}

func (s *historyQueryStub) ReadVisibleAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, int64, error) {
	return nil, 0, nil
}

func (s *historyQueryStub) ReadVisibleBeforeSeq(_ context.Context, beforeSeq int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
	s.calls++
	end := len(s.rows)
	if beforeSeq > 0 {
		for end > 0 && s.rows[end-1].Seq >= beforeSeq {
			end--
		}
	}
	start := end - limit
	if start < 0 {
		start = 0
	}
	page := append([]storespec.StoredRow(nil), s.rows[start:end]...)
	head := int64(0)
	if len(s.rows) > 0 {
		head = s.rows[len(s.rows)-1].Seq
	}
	return page, head, start > 0, nil
}

func historyRow(seq int64, id string, kind message.Kind, parent, correlation string, terminal bool) storespec.StoredRow {
	return storespec.StoredRow{
		Seq: seq,
		Envelope: message.Envelope{
			ID:            message.ID(id),
			Kind:          kind,
			ParentID:      message.ID(parent),
			CorrelationID: message.ID(correlation),
			Type:          "history.test",
		},
		IsTerminal: terminal,
	}
}

func appendCompletedRoot(rows []storespec.StoredRow, seq *int64, name string, progress int, withChild bool) []storespec.StoredRow {
	root := name + "-root"
	rows = append(rows, historyRow(*seq, root, message.KindRequest, "", root, false))
	*seq++
	for index := 0; index < progress; index++ {
		rows = append(rows, historyRow(*seq, fmt.Sprintf("%s-progress-%03d", name, index), message.KindResponse, root, root, false))
		*seq++
	}
	if withChild {
		child := name + "-child"
		rows = append(rows, historyRow(*seq, child, message.KindRequest, root, root, false))
		*seq++
		rows = append(rows, historyRow(*seq, name+"-child-terminal", message.KindResponse, child, root, true))
		*seq++
	}
	rows = append(rows, historyRow(*seq, name+"-terminal", message.KindResponse, root, root, true))
	*seq++
	return rows
}

func TestHistoryWindowScansToThreeCompleteRootTurnsAndProjectsProgress(t *testing.T) {
	seq := int64(1)
	var rows []storespec.StoredRow
	rows = appendCompletedRoot(rows, &seq, "one", 4, false)
	rows = appendCompletedRoot(rows, &seq, "two", 4, true)
	rows = appendCompletedRoot(rows, &seq, "three", 4, false)
	rows = appendCompletedRoot(rows, &seq, "four", 300, false)
	query := &historyQueryStub{rows: rows}

	window, err := readVisibleTurnWindow(context.Background(), query, channelspec.HistoryWindowQuery{
		TargetRows:           200,
		MinimumCompleteRoots: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if query.calls < 2 {
		t.Fatalf("raw tail cut the newest root, expected another backwards scan; calls=%d", query.calls)
	}
	if got, want := window.OldestSeq, int64(7); got != want {
		t.Fatalf("oldest seq = %d, want root two at %d", got, want)
	}
	if !window.HasOlder {
		t.Fatal("expected the first complete root to remain on an older page")
	}

	rootRequests := 0
	responses := 0
	childSeen := false
	for _, row := range window.Rows {
		if row.Envelope.Kind == message.KindRequest && row.Envelope.ParentID == "" {
			rootRequests++
		}
		if row.Envelope.Kind == message.KindResponse {
			responses++
		}
		if row.Envelope.ID == "two-child" {
			childSeen = true
		}
	}
	if rootRequests != 3 {
		t.Fatalf("root requests = %d, want 3 (child requests must not count)", rootRequests)
	}
	if responses != 4 {
		t.Fatalf("responses = %d, want the 3 root terminals plus child terminal", responses)
	}
	if !childSeen {
		t.Fatal("history projection dropped a nested request")
	}
}

func TestHistoryWindowKeepsLatestProgressOnlyForOpenRequest(t *testing.T) {
	seq := int64(1)
	var rows []storespec.StoredRow
	rows = appendCompletedRoot(rows, &seq, "one", 2, false)
	rows = appendCompletedRoot(rows, &seq, "two", 2, false)
	rows = appendCompletedRoot(rows, &seq, "three", 2, false)
	openRoot := "open-root"
	rows = append(rows, historyRow(seq, openRoot, message.KindRequest, "", openRoot, false))
	seq++
	rows = append(rows,
		historyRow(seq, "open-progress-1", message.KindResponse, openRoot, openRoot, false),
		historyRow(seq+1, "open-progress-2", message.KindResponse, openRoot, openRoot, false),
	)
	query := &historyQueryStub{rows: rows}

	window, err := readVisibleTurnWindow(context.Background(), query, channelspec.HistoryWindowQuery{
		TargetRows:           2,
		MinimumCompleteRoots: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	seenFirst := false
	seenLatest := false
	for _, row := range window.Rows {
		seenFirst = seenFirst || row.Envelope.ID == "open-progress-1"
		seenLatest = seenLatest || row.Envelope.ID == "open-progress-2"
	}
	if seenFirst || !seenLatest {
		t.Fatalf("open progress projection: first=%v latest=%v", seenFirst, seenLatest)
	}
}

func TestHistoryWindowPaginationUsesExclusiveRootBoundary(t *testing.T) {
	seq := int64(1)
	var rows []storespec.StoredRow
	for index := 1; index <= 7; index++ {
		rows = appendCompletedRoot(rows, &seq, fmt.Sprintf("turn-%d", index), 2, false)
	}
	query := &historyQueryStub{rows: rows}

	newer, err := readVisibleTurnWindow(context.Background(), query, channelspec.HistoryWindowQuery{
		TargetRows:           6,
		MinimumCompleteRoots: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	older, err := readVisibleTurnWindow(context.Background(), query, channelspec.HistoryWindowQuery{
		BeforeSeq:            newer.OldestSeq,
		TargetRows:           6,
		MinimumCompleteRoots: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !newer.HasOlder || !older.HasOlder {
		t.Fatalf("unexpected pagination flags: newer=%v older=%v", newer.HasOlder, older.HasOlder)
	}
	if older.NewestSeq >= newer.OldestSeq {
		t.Fatalf("pages overlap: older newest=%d, newer oldest=%d", older.NewestSeq, newer.OldestSeq)
	}
	oldest, err := readVisibleTurnWindow(context.Background(), query, channelspec.HistoryWindowQuery{
		BeforeSeq:            older.OldestSeq,
		TargetRows:           6,
		MinimumCompleteRoots: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldest.HasOlder {
		t.Fatal("oldest page still reports older history")
	}
	if oldest.NewestSeq >= older.OldestSeq {
		t.Fatalf("oldest pages overlap: oldest newest=%d, older oldest=%d", oldest.NewestSeq, older.OldestSeq)
	}
}

func TestHistoryWindowBoundsStandaloneEvents(t *testing.T) {
	rows := make([]storespec.StoredRow, 12)
	for index := range rows {
		rows[index] = historyRow(int64(index+1), fmt.Sprintf("event-%d", index+1), message.KindEvent, "", "", false)
	}
	window, err := readVisibleTurnWindow(context.Background(), &historyQueryStub{rows: rows}, channelspec.HistoryWindowQuery{TargetRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(window.Rows) != 3 || window.OldestSeq != 10 || !window.HasOlder {
		t.Fatalf("standalone page = rows:%d oldest:%d older:%v", len(window.Rows), window.OldestSeq, window.HasOlder)
	}
}
