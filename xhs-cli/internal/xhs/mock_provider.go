package xhs

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// MockProvider 返回固定 fixture 数据，覆盖 5 个命令。
//
// 设计借鉴 cli/src/lib/backends/mock.ts 与 cli/src/mock/fixtures/*.yaml。
// 不读 yaml 文件，全部 hardcode 在代码里：worker 不需要为 binary 单独打包资源。
type MockProvider struct {
	// now 是测试可注入的时钟；零值用 time.Now().UTC()。
	now func() time.Time
	// entropy 是测试可注入的 ULID 随机源；零值用 crypto/rand。
	entropy func() ([]byte, error)
}

// NewMockProvider 构造默认 MockProvider。
func NewMockProvider() *MockProvider {
	return &MockProvider{}
}

// Name 实现 Provider.Name。
func (m *MockProvider) Name() string { return "mock" }

func (m *MockProvider) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now().UTC()
}

// Publish 用 ulid 生成新 note_id 并返回固定 url。
func (m *MockProvider) Publish(_ context.Context, args PublishArgs) (any, error) {
	if strings.TrimSpace(args.Title) == "" {
		return nil, &CodeError{Code: "invalid_argument", Msg: "title is required"}
	}
	now := m.clock()
	noteID, err := m.newULID(now)
	if err != nil {
		return nil, fmt.Errorf("generate ulid: %w", err)
	}
	return PublishResult{
		NoteID:      noteID,
		URL:         fmt.Sprintf("https://xhs.com/explore/%s", noteID),
		PublishedAt: now.Format(time.RFC3339),
	}, nil
}

// Search 返回 fixture（与 cli/src/mock/fixtures/mock-search-奶茶.yaml 同构）。
func (m *MockProvider) Search(_ context.Context, args SearchArgs) (any, error) {
	if strings.TrimSpace(args.Keyword) == "" {
		return nil, &CodeError{Code: "invalid_argument", Msg: "keyword is required"}
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 10
	}
	all := []SearchItem{
		{NoteID: "01HXYZ", Title: fmt.Sprintf("%s探店：三家热门店谁更稳", args.Keyword), Author: "糖分研究所", Likes: 128},
		{NoteID: "01HXZA", Title: fmt.Sprintf("自制%s清单：通勤路上不踩雷", args.Keyword), Author: "日常续命", Likes: 87},
	}
	if limit < len(all) {
		all = all[:limit]
	}
	return SearchResult{Results: all}, nil
}

// GetMyRecent 返回 fixture（与 cli/src/mock/fixtures/mock-recent.yaml 同构）。
func (m *MockProvider) GetMyRecent(_ context.Context, args GetMyRecentArgs) (any, error) {
	limit := args.Limit
	if limit <= 0 {
		limit = 10
	}
	all := []RecentItem{
		{NoteID: "01HXYZ", Title: "四月穿搭随手记", URL: "https://xhs.com/explore/01HXYZ", PublishedAt: "2026-04-28T09:30:00Z"},
		{NoteID: "01HXZA", Title: "奶茶探店：三家热门店谁更稳", URL: "https://xhs.com/explore/01HXZA", PublishedAt: "2026-04-27T13:00:00Z"},
		{NoteID: "01HXZB", Title: "工作日早餐备忘录", URL: "https://xhs.com/explore/01HXZB", PublishedAt: "2026-04-26T07:15:00Z"},
	}
	if limit < len(all) {
		all = all[:limit]
	}
	return GetMyRecentResult{Notes: all}, nil
}

// GetNote 返回 fixture（与 cli/src/mock/fixtures/mock-note-01HXYZ.yaml 同构）。
// 仅当 note_id 匹配 fixture 时返回数据；其他 id 返回 not_found。
func (m *MockProvider) GetNote(_ context.Context, args GetNoteArgs) (any, error) {
	if strings.TrimSpace(args.NoteID) == "" {
		return nil, &CodeError{Code: "invalid_argument", Msg: "note_id is required"}
	}
	if args.NoteID != "01HXYZ" {
		return nil, &CodeError{Code: "note_not_found", Msg: fmt.Sprintf("Mock note %q not found", args.NoteID)}
	}
	return GetNoteResult{
		Note: NoteBody{
			Title:   "四月穿搭随手记",
			Content: "这是一条用于 Phase A 验收的 mock 笔记。",
			Metrics: NoteMetrics{Likes: 321, Comments: 18, Collects: 64},
		},
	}, nil
}

// PublishStatus 把 note_id 映射成 published 状态 + url。
func (m *MockProvider) PublishStatus(_ context.Context, args PublishStatusArgs) (any, error) {
	if strings.TrimSpace(args.NoteID) == "" {
		return nil, &CodeError{Code: "invalid_argument", Msg: "note_id is required"}
	}
	return PublishStatusResult{
		Status: "published",
		URL:    fmt.Sprintf("https://xhs.com/explore/%s", args.NoteID),
	}, nil
}

// ---- helpers ----

// newULID 生成一个 ULID 字符串（26 字符 Crockford base32 大写）。
func (m *MockProvider) newULID(now time.Time) (string, error) {
	id, err := ulid.New(ulid.Timestamp(now), cryptorand.Reader)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// CodeError 是带显式 code 的业务错误，CLI 层会直接映射到 envelope.error.{code,message}。
type CodeError struct {
	Code string
	Msg  string
}

func (e *CodeError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}
