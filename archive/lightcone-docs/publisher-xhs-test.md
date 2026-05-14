# 小红书发布真实环境验证清单

## 前置条件

- 主服务已启动，并能访问当前 daemon。
- daemon 机器安装 Chrome 或 Chromium。
- 测试用户已在「连接外部账号」中完成小红书扫码登录。
- 对应 Agent 已绑定 `xhs-post` skill，并已获得该小红书凭证授权。
- 测试图片或视频文件位于该 Agent 的 workspace 内，不能使用 workspace 外部路径。

## 图文发布验证

1. 在 lightcone 中向绑定 `xhs-post` 的 Agent 发起发布请求，包含标题、正文、话题和 1 张 workspace 内图片。
2. Agent 应先调用 `request_approval`，界面应出现审批卡片。
3. 人类批准后，Agent 应调用 `execute_approved_action`。
4. Agent 随后调用 `publish_content`，并传入同一个 `approval_action_id`。
5. 期望结果：
   - 未批准时，`publish_content` 拒绝发布。
   - 媒体路径在 workspace 外时，`publish_content` 拒绝发布。
   - 发布中不允许同一小红书 profile 同时发起扫码登录。
   - 发布成功后 pending action 状态为 `executed`。
   - 发布失败后 pending action 状态为 `failed`，并带错误信息。

## 登录/发布互斥验证

1. 启动一次小红书 browser-login，不扫码，保持登录窗口在等待状态。
2. 同时让 Agent 执行已批准的小红书发布。
3. 期望结果：发布等待 profile 锁，超时后返回 profile busy 错误，不清空或破坏登录中的 profile。

## 需要记录的信息

如果发布失败，请保存以下信息：

- Agent 收到的错误文本。
- daemon 日志中 `[publisher]`、`[ChromePool]`、`[XhsAdapter]` 相关行。
- 小红书页面截图。
- 当前 URL。
- 页面上可见的提示文案，例如上传失败、认证、风控、审核提示。

## 选择器校准点

如果小红书页面改版，优先校准这些行为：

- 发布页 URL 是否仍为 `https://creator.xiaohongshu.com/publish/publish`。
- 上传区域是否仍包含 `input[type="file"]`。
- 标题输入框是否能通过 `[class*="title"] input, [placeholder*="标题"]` 找到。
- 正文输入区是否能通过 `[class*="content"] [contenteditable], [placeholder*="正文"], [class*="editor"]` 找到。
- 发布按钮文案是否仍包含 `发布`。
