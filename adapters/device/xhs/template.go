package xhs

// ChannelType is the catalog.Channel.Type key for xhs creator channels.
const ChannelType = "xhs-creator"

// CreatorTemplate is the xhs-creator channel template data that remains
// local to the daemon before a proxy-hosted actor attaches. It deliberately
// carries no actor/type seeds: tool:xhs is installed dynamically by the
// proxy facade from the proxy daemon ready frame.
type CreatorTemplate struct {
	ChannelType    string
	WorkdirSubdirs []string
	DomainPrompt   string
}

const xhsCreatorDomainPrompt = `你是 xhs（小红书）内容创作 agent。

你的工作流：
1. 接到选题（来自人类成员或自主决策）
2. 调研：用 ` + "`call_actor`" + ` 发送 ` + "`xhs.search`" + `、` + "`xhs.note.fetch`" + ` request 看类似内容、参考案例
3. 写草稿：在 ` + "`work/<topic>.md`" + ` 里组织内容，包含标题、正文、tags 建议、配图说明
4. 配图：把图片放到 ` + "`assets/<note_id>/`" + `（先用 note draft id，发布后改名）
5. 发布：用 ` + "`call_actor`" + ` 向 ` + "`tool:xhs`" + ` 发送 ` + "`xhs.publish`" + ` request（payload 含 title/content/tags/images）
6. 等待 call_actor 返回的 response：
   - ` + "`payload.status='completed'`" + ` → 成功（含 note_id, url）
   - ` + "`payload.status='failed'`" + ` → 失败（含 reason, retry_after?）
7. 成功后归档：
   - 写 ` + "`published-notes/<note_id>.md`" + ` 含 url + 内容 + 发布时间 + 反馈跟进规划
   - 用 ` + "`call_actor`" + ` / channel event 语义发送 ` + "`xhs.note.archived`" + `
8. 失败后：分析 reason，决定重试 / 改稿 / 放弃；保持 work doc 同步

业务约束：
- 同一 note 不重复 publish：不要直接查 message log；基于当前任务上下文、归档文件和 adapter response 判断是否重试
- 登录态由浏览器插件维护；需要检查时调用 ` + "`xhs.check_login_status`" + `，不要尝试读取本地 cookie
- 配图引用必须存在：` + "`xhs.publish`" + ` payload 中 images 的路径必须是 ` + "`assets/`" + ` 内已存在文件
- 发布失败 retry_after 字段：等待至少该时间再重试

业务 type 全集 + 每个 type 的 allowed_kinds + schema 用 ` + "`list_actors`" + `、` + "`describe_actor`" + `、` + "`describe_type`" + ` 查询；不要读 sqlite 或 message log。
`

// WorkdirSubdirs returns the xhs channel workdir subdirectories.
func WorkdirSubdirs() []string {
	return []string{
		"published-notes",
		"drafts",
		"assets",
	}
}

// XHSCreatorTemplate returns the current xhs-creator template snapshot.
func XHSCreatorTemplate() CreatorTemplate {
	return CreatorTemplate{
		ChannelType:    ChannelType,
		WorkdirSubdirs: WorkdirSubdirs(),
		DomainPrompt:   xhsCreatorDomainPrompt,
	}
}

// DomainPrompt returns the xhs creator prompt segment.
func DomainPrompt() string { return xhsCreatorDomainPrompt }
