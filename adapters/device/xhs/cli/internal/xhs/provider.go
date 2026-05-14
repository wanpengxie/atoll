// Package xhs 定义 coagent-xhs 的 Provider 抽象与共享类型。
//
// Provider 暴露 5 个 xhs 业务能力：publish / search / recent / get-note /
// sync-cookie。两种 backend：
//
//   - MockProvider：返回固定 fixture（直接同步结果）。
//   - RealProvider：把命令翻译成 v4 message envelope —— 通过 spawn
//     `coagent ask --type xhs.<op> --audience tool:xhs-adapter` 子进程
//     发到 daemon RPC。立即返回 dispatch ack（含 correlation_id），
//     不阻塞等真实结果。
//
// 因为 mock 与 real 的"返回数据形态"不同（mock=业务 payload；real=dispatch ack），
// Provider 方法统一返回 `any`：CLI 层把它原样塞到 envelope.data。这样 mock 和 real
// 共享同一接口，CLI 命令实现保持一致。
//
// **M1.3-T14 变更**：legacy `device.command.send` RPC 入口下线；real
// provider 改为 spawn `coagent` CLI（L4 §2.3.2 "CLI 是 daemon RPC
// 的 domain wrapper"）。`publish-status` 命令在 v4 type 表里没有对应
// type，已废弃；新增 `sync-cookie`（对应 `xhs.cookie.sync`）。type
// 名也按 L4 §2.1 重命名：
//
//   - xhs.get-my-recent  → xhs.recent.fetch
//   - xhs.get-note       → xhs.note.fetch
//   - xhs.publish-status → deleted
//   - +                  xhs.cookie.sync
package xhs

import "context"

// Provider 是 xhs CLI 的统一业务接口。
//
// 返回值用 `any`：mock 模式下是业务结构体（PublishResult / SearchResult / ...），
// real 模式下是 DispatchAck。CLI 层不关心，直接当 envelope.data 序列化输出。
type Provider interface {
	// Name 返回 provider 名称（用于诊断）。
	Name() string

	// Publish 发布一篇笔记（v4 type: xhs.publish）。
	Publish(ctx context.Context, args PublishArgs) (any, error)
	// Search 关键词搜索（v4 type: xhs.search）。
	Search(ctx context.Context, args SearchArgs) (any, error)
	// GetMyRecent 获取当前账号最近发布列表（v4 type: xhs.recent.fetch）。
	// 方法名保留 GetMyRecent 以兼容现有 CLI；v4 type 已重命名。
	GetMyRecent(ctx context.Context, args GetMyRecentArgs) (any, error)
	// GetNote 按 note_id / url 拉笔记详情（v4 type: xhs.note.fetch）。
	GetNote(ctx context.Context, args GetNoteArgs) (any, error)
	// SyncCookie 同步 cookie（v4 type: xhs.cookie.sync）。无参数。
	SyncCookie(ctx context.Context, args SyncCookieArgs) (any, error)
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

// GetMyRecentArgs 是 recent 命令的参数。
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

// SyncCookieArgs 是 sync-cookie 命令的参数。v4 `xhs.cookie.sync` 类型
// 的 schema 没有 required 字段，保留空结构以方便未来扩展（例如
// account_id / force flag）。
type SyncCookieArgs struct{}

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

// RecentItem 是 recent 单条。
type RecentItem struct {
	NoteID      string `json:"note_id"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	PublishedAt string `json:"published_at"`
}

// GetMyRecentResult 是 recent 结果集。
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

// SyncCookieResult 是 sync-cookie 命令在 mock 模式下的返回。
type SyncCookieResult struct {
	Status      string `json:"status"`       // "ok" | "expired"
	LastSyncAt  string `json:"last_sync_at"` // RFC3339
	CookieCount int    `json:"cookie_count"` // 同步到的 cookie 数量
}

// ---- real result（5 命令统一 dispatch ack） ----

// DispatchAck 是 RealProvider 5 命令立即返回给 agent 的统一 ack。
//
// 字段含义：
//   - CorrelationID  daemon 生成的 envelope correlation_id（agent 用这个 poll
//     message store 查后续 response）
//   - ID             envelope id（M1.3 baseline 与 correlation_id 一致；保留
//     字段方便未来 explicit correlation 模式）
//   - Status         "dispatched"（命令已写入 channel，但尚未拿到 adapter
//     response）
//   - Dedupe         daemon harness step 0.5 dedupe 命中标识
type DispatchAck struct {
	CorrelationID string `json:"correlation_id"`
	ID            string `json:"id,omitempty"`
	Status        string `json:"status"`
	Dedupe        bool   `json:"dedupe,omitempty"`
}
