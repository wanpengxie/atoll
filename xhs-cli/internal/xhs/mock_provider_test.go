package xhs

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMockProvider_Publish(t *testing.T) {
	p := NewMockProvider()
	out, err := p.Publish(context.Background(), PublishArgs{
		Title:       "T",
		ContentPath: "x.md",
		Content:     "body",
	})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	res, ok := out.(PublishResult)
	if !ok {
		t.Fatalf("expected PublishResult, got %T", out)
	}
	if res.NoteID == "" {
		t.Fatal("note_id empty")
	}
	if !strings.HasPrefix(res.URL, "https://xhs.com/explore/") {
		t.Fatalf("url not prefixed: %q", res.URL)
	}
	if res.PublishedAt == "" {
		t.Fatal("published_at empty")
	}
}

func TestMockProvider_Publish_RejectsEmptyTitle(t *testing.T) {
	p := NewMockProvider()
	_, err := p.Publish(context.Background(), PublishArgs{Title: "", Content: "x"})
	if err == nil {
		t.Fatal("expected error for empty title")
	}
	var ce *CodeError
	if !errors.As(err, &ce) || ce.Code != "invalid_argument" {
		t.Fatalf("expected CodeError invalid_argument, got %v", err)
	}
}

func TestMockProvider_Search(t *testing.T) {
	p := NewMockProvider()
	out, err := p.Search(context.Background(), SearchArgs{Keyword: "奶茶", Limit: 1})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	res := out.(SearchResult)
	if len(res.Results) != 1 {
		t.Fatalf("limit=1 expect 1 result, got %d", len(res.Results))
	}
	if res.Results[0].NoteID != "01HXYZ" {
		t.Fatalf("first note_id mismatch: %q", res.Results[0].NoteID)
	}
}

func TestMockProvider_GetMyRecent(t *testing.T) {
	p := NewMockProvider()
	out, err := p.GetMyRecent(context.Background(), GetMyRecentArgs{Limit: 2})
	if err != nil {
		t.Fatalf("get-my-recent error: %v", err)
	}
	res := out.(GetMyRecentResult)
	if len(res.Notes) != 2 {
		t.Fatalf("limit=2 expect 2 notes, got %d", len(res.Notes))
	}
}

func TestMockProvider_GetNote(t *testing.T) {
	p := NewMockProvider()

	out, err := p.GetNote(context.Background(), GetNoteArgs{NoteID: "01HXYZ"})
	if err != nil {
		t.Fatalf("get-note error: %v", err)
	}
	res := out.(GetNoteResult)
	if res.Note.Title == "" {
		t.Fatal("expected note title")
	}
	if res.Note.Metrics.Likes == 0 {
		t.Fatal("expected non-zero likes")
	}

	_, err = p.GetNote(context.Background(), GetNoteArgs{NoteID: "missing"})
	if err == nil {
		t.Fatal("expected not_found error for unknown note")
	}
	var ce *CodeError
	if !errors.As(err, &ce) || ce.Code != "note_not_found" {
		t.Fatalf("expected note_not_found, got %v", err)
	}
}

func TestMockProvider_SyncCookie(t *testing.T) {
	p := NewMockProvider()
	out, err := p.SyncCookie(context.Background(), SyncCookieArgs{})
	if err != nil {
		t.Fatalf("sync-cookie error: %v", err)
	}
	res := out.(SyncCookieResult)
	if res.Status != "ok" {
		t.Fatalf("expected status=ok, got %q", res.Status)
	}
	if res.CookieCount <= 0 {
		t.Fatalf("expected positive cookie count; got %d", res.CookieCount)
	}
	if res.LastSyncAt == "" {
		t.Fatalf("expected last_sync_at to be populated")
	}
}

func TestMockProvider_Name(t *testing.T) {
	if NewMockProvider().Name() != "mock" {
		t.Fatal("mock provider name mismatch")
	}
}
