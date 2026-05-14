---
name: x-post
description: 发布推文到 X (Twitter)，图文均支持。所有发布操作需经人类审批。
tags: ["x", "twitter", "publishing", "social-media"]
mcp_config: {"server":"platform","platform":"x","command":"node","args":["{platform_mcp_path}"],"env":["X_API_KEY","X_API_SECRET","X_ACCESS_TOKEN","X_ACCESS_SECRET"]}
---

# X (Twitter) 发帖能力

## 工作流程（必须遵守）

所有发布操作都需要人类审批，不得跳过：

1. 起草推文内容
2. 调用 `request_approval` 提交审批请求
3. 等待人类审批（继续处理其他消息，不要阻塞）
4. 收到 "Action approved" 通知后，调用 `execute_approved_action`
5. 调用 `platform_post(platform="x", content="...")` 执行发布

## 内容规范

- 单条推文不超过 280 字符（含空格）
- 图片附件：正式图片产出先保存到 workspace 的 `artifacts/`；发布时如需公网 URL，再用 `upload_image` 生成临时公开 URL 并传给 `media_urls` 参数
- 不允许在未经审批的情况下直接发布任何内容

## 示例调用

```
request_approval(
  action_type="x_post",
  platform="x",
  description="发布关于 AI 行业早报的推文",
  payload={
    "content": "今日 AI 行业早报：...",
    "media_urls": []
  }
)
```

审批通过后：
```
execute_approved_action(action_id="<从 request_approval 返回的 ID>")
platform_post(platform="x", content="今日 AI 行业早报：...")
```

## 账号验证

可随时调用 `platform_get_account(platform="x")` 验证凭证有效性及账号信息。
