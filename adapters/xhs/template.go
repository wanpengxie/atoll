package xhs

import "github.com/wanpengxie/ActOS/kernel/actorreg"

// ChannelType is the catalog.Channel.Type key the L4 xhs-creator
// template binds (v4-layer4-spec §2). Re-exported here so cmd/daemon /
// gateway / future template registry pin one constant.
const ChannelType = "xhs-creator"

// Template is the L4 channel-template snapshot for the xhs-creator
// channel type (M1.6-T5 phase-2). The adapter package owns the
// template definition because the actor seed + workdir subdirs +
// declared types are all adapter-internal facts; the composition root
// (cmd/daemon) converts this value into the runtime.ChannelTemplate it
// wires into DaemonConfig.ChannelTemplates without taking a runtime
// dependency on this package.
//
// Authoritative spec: .dalek/pm/v4-layer4-spec.md §0.2 / §1.x / §2.x.
// Each Template entry is read-only; constructor functions return fresh
// slices so callers may not mutate the canonical seeds.
type Template struct {
	// ChannelType is the catalog.Channel.Type key clients pass on
	// channel.create. Always "xhs-creator" for this template.
	ChannelType string

	// AdapterActorSeeds lists the actor_registry rows the bootstrap
	// saga inserts during step 5b so framework.Manager.Install can
	// locate the adapter actor with the right binding.
	AdapterActorSeeds []actorreg.Record

	// WorkdirSubdirs lists relative directory paths the bootstrap
	// saga mkdirs inside <ChannelsDir>/<channelID>/ during step 5c.
	// Per v4-layer4-spec §2.5: published-notes/, drafts/, assets/.
	WorkdirSubdirs []string

	// DomainPrompt is the L4 §2.4 prompt segment cmd/daemon injects
	// into the worker base prompt at spawn time (M1.6-T5 phase-3).
	DomainPrompt string
}

// xhsCreatorDomainPrompt is the L4 §2.4 prompt segment cmd/daemon
// injects into the worker base prompt at spawn time. Kept as a raw
// string constant so the prompt-cache stays stable across builds and
// can be hashed for grep-able telemetry (M1.6-T5 acceptance B3).
//
// Authoritative source: .dalek/pm/v4-layer4-spec.md §2.4 (verbatim).
const xhsCreatorDomainPrompt = `你是 xhs（小红书）内容创作 agent。

你的工作流：
1. 接到选题（来自人类成员或自主决策）
2. 调研：调 ` + "`xhs search`" + `、` + "`xhs get-note`" + ` 看类似内容、参考案例（这些都是 kind=request，response 在后续 turn 回到你）
3. 写草稿：在 ` + "`work/<topic>.md`" + ` 里组织内容，包含标题、正文、tags 建议、配图说明
4. 配图：把图片放到 ` + "`assets/<note_id>/`" + `（先用 note draft id，发布后改名）
5. 发布：调 ` + "`xhs publish --title ... --content ... --tags ...`" + `（kind=request）
6. 等待 response message（同 correlation_id, kind=response）：
   - ` + "`payload.status='completed'`" + ` → 成功（含 note_id, url）
   - ` + "`payload.status='failed'`" + ` → 失败（含 reason, retry_after?）
7. 成功后归档：
   - 写 ` + "`published-notes/<note_id>.md`" + ` 含 url + 内容 + 发布时间 + 反馈跟进规划
   - ` + "`coagent emit --type xhs.note.archived --payload '{\"note_id\":\"...\",\"archive_path\":\"published-notes/<id>.md\"}'`" + `
8. 失败后：分析 reason，决定重试 / 改稿 / 放弃；保持 work doc 同步

业务约束：
- 同一 note 不重复 publish：发布前 query ` + "`messages WHERE type='xhs.publish' AND kind='response' AND json_extract(payload, '$.status')='completed' AND json_extract(payload, '$.note_id')=...`" + `
- cookie 需保持有效：定期 ` + "`xhs sync-cookie`" + `，发现失效时主动同步
- 配图引用必须存在：` + "`xhs publish --images path[]`" + ` 的路径必须是 ` + "`assets/`" + ` 内已存在文件
- 发布失败 retry_after 字段：等待至少该时间再重试

业务 type 你能发的：
- request 类（调外部工具）：xhs.publish / xhs.search / xhs.note.fetch / xhs.recent.fetch / xhs.cookie.sync
- event 类（自报事实）：xhs.note.archived

业务 type 全集 + 每个 type 的 allowed_kinds + schema 看：
` + "`sqlite3 messages.sqlite \"SELECT type, allowed_kinds, schemas_by_kind FROM type_registry WHERE domain='xhs'\"`" + `。
`

// WorkdirSubdirs returns the per-v4-layer4-spec §2.5 directory list
// the bootstrap saga mkdirs alongside the channel sqlite. Returned as
// a fresh slice so callers can append safely.
func WorkdirSubdirs() []string {
	return []string{
		"published-notes",
		"drafts",
		"assets",
	}
}

// XHSCreatorTemplate returns the static Template snapshot for the
// xhs-creator channel type. cmd/daemon converts this into
// runtime.ChannelTemplate via the conversion helper in
// cmd/daemon/adapter_wiring.go.
//
// The adapter actor seed list ships exactly one row — tool:xhs-adapter
// with kind=tool, binding=in_process — matching the M1.6-T2 in_process
// scaffold. A later phase swaps the seed binding to via_server_transit
// (DeviceXHSActorSeed) by editing this single function; no other
// caller needs to change.
func XHSCreatorTemplate() Template {
	return Template{
		ChannelType:       ChannelType,
		AdapterActorSeeds: []actorreg.Record{DefaultActorSeed()},
		WorkdirSubdirs:    WorkdirSubdirs(),
		DomainPrompt:      xhsCreatorDomainPrompt,
	}
}

// DomainPrompt re-exports the prompt constant for tests / telemetry
// that need to hash or grep its content (M1.6-T5 acceptance B3).
func DomainPrompt() string { return xhsCreatorDomainPrompt }
