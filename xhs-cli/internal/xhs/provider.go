// Package xhs 定义 coagent-xhs 的 Provider 抽象与共享类型。
//
// Provider 暴露 5 个 xhs 业务能力：publish / search / get-my-recent / get-note /
// publish-status。两种 backend：
//
//   - MockProvider：返回固定 fixture（直接同步结果）。
//   - RealProvider：把命令翻译成 daemon HTTP RPC `device.command.send`，
//     立即返回 dispatch ack（含 correlation_id），不阻塞等真实结果。
//
// 因为 mock 与 real 的"返回数据形态"不同（mock=业务 payload；real=dispatch ack），
// Provider 方法统一返回 `any`：CLI 层把它原样塞到 envelope.data。这样 mock 和 real
// 共享同一接口，CLI 命令实现保持一致。
package xhs

import "context"

// Provider 是 xhs CLI 的统一业务接口。
//
// 返回值用 `any`：mock 模式下是业务结构体（PublishResult / SearchResult / ...），
// real 模式下是 DispatchAck。CLI 层不关心，直接当 envelope.data 序列化输出。
type Provider interface {
	// Name 返回 provider 名称（用于诊断）。
	Name() string

	// Publish 发布一篇笔记。
	Publish(ctx context.Context, args PublishArgs) (any, error)
	// Search 关键词搜索。
	Search(ctx context.Context, args SearchArgs) (any, error)
	// GetMyRecent 获取当前账号最近发布列表。
	GetMyRecent(ctx context.Context, args GetMyRecentArgs) (any, error)
	// GetNote 按 note_id 拉笔记详情。
	GetNote(ctx context.Context, args GetNoteArgs) (any, error)
	// PublishStatus 查询某个发布任务的状态。
	PublishStatus(ctx context.Context, args PublishStatusArgs) (any, error)
}

// ---- args ----

// PublishArgs 是 publish 命令的参数。
//
// 字段约定（mock vs real）：
//   - mock 模式：CLI 读 ContentPath 文件 → Content；Images 用 string paths（mock 不消费）；ImageData 留空。
//   - real 模式：CLI 不读 content 文件，只把 ContentPath 解析为 absolute path 传给 daemon；Content 留空；
//     Images 留空；ImageData 是逐个 base64 编码后的归一化对象数组，对齐 extension publish-content.ts 期望。
type PublishArgs struct {
	Title       string           `json:"title"`
	ContentPath string           `json:"content_path,omitempty"` // 文件路径（real 模式必填且为 absolute）
	Content     string           `json:"content,omitempty"`      // 内联正文（仅 mock 用，real 模式不发）
	Images      []string         `json:"images,omitempty"`       // 文件路径数组（仅 mock 用）
	ImageData   []map[string]any `json:"image_data,omitempty"`   // 归一化后的图片对象（仅 real 用，发到 RPC）
	Tags        []string         `json:"tags,omitempty"`
}

// SearchArgs 是 search 命令的参数。
type SearchArgs struct {
	Keyword string `json:"keyword"`
	Limit   int    `json:"limit,omitempty"`
}

// GetMyRecentArgs 是 get-my-recent 命令的参数。
type GetMyRecentArgs struct {
	Limit int `json:"limit,omitempty"`
}

// GetNoteArgs 是 get-note 命令的参数。
//
// 字段约定：
//   - mock 模式：仅消费 NoteID（fixture 路径）。
//   - real 模式：URL / XsecToken 至少其一非空，CLI 会把三者一起塞 RPC params；
//     daemon → extension 由 extension 决定如何 fallback（见 spec §4.1 xhs.get-note）。
type GetNoteArgs struct {
	NoteID    string `json:"note_id,omitempty"`
	URL       string `json:"url,omitempty"`
	XsecToken string `json:"xsec_token,omitempty"`
}

// PublishStatusArgs 是 publish-status 命令的参数。
type PublishStatusArgs struct {
	NoteID string `json:"note_id"`
}

// ---- mock results (与 cli/src/mock/fixtures/*.yaml 同构) ----

// PublishResult 是 mock 模式下 publish 立即返回的结构。
type PublishResult struct {
	NoteID      string `json:"note_id"`
	URL         string `json:"url"`
	PublishedAt string `json:"published_at"`
}

// SearchItem 是 search 单条结果。
type SearchItem struct {
	NoteID string `json:"note_id"`
	Title  string `json:"title"`
	Author string `json:"author"`
	Likes  int    `json:"likes"`
}

// SearchResult 是 search 结果集。
type SearchResult struct {
	Results []SearchItem `json:"results"`
}

// RecentItem 是 get-my-recent 单条。
type RecentItem struct {
	NoteID      string `json:"note_id"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	PublishedAt string `json:"published_at"`
}

// GetMyRecentResult 是 get-my-recent 结果集。
type GetMyRecentResult struct {
	Notes []RecentItem `json:"notes"`
}

// NoteMetrics 是 get-note 中的指标段。
type NoteMetrics struct {
	Likes    int `json:"likes"`
	Comments int `json:"comments"`
	Collects int `json:"collects"`
}

// NoteBody 是 get-note 中的笔记主体。
type NoteBody struct {
	Title   string      `json:"title"`
	Content string      `json:"content"`
	Metrics NoteMetrics `json:"metrics"`
}

// GetNoteResult 是 get-note 命令结果。
type GetNoteResult struct {
	Note NoteBody `json:"note"`
}

// PublishStatusResult 是 publish-status 命令结果。
type PublishStatusResult struct {
	Status string `json:"status"`
	URL    string `json:"url"`
}

// ---- real result（5 命令统一 dispatch ack） ----

// DispatchAck 是 RealProvider 5 命令立即返回给 agent 的统一 ack。
//
// 字段含义：
//   - CorrelationID  daemon 生成的 dispatch correlation id（agent 用这个 poll）
//   - Status         "dispatched"（命令已下派但未完成）
//   - SelfCheck      可选，daemon 返回的 self-check schedule 元信息
type DispatchAck struct {
	CorrelationID string         `json:"correlation_id"`
	Status        string         `json:"status"`
	SelfCheck     map[string]any `json:"self_check,omitempty"`
}
