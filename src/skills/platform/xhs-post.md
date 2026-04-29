---
name: xhs-post
description: 在小红书发布图文笔记或短视频。所有发布操作需经人类审批。
tags: ["xhs", "xiaohongshu", "publishing", "social-media", "browser"]
mcp_config: {"server":"publisher","platform":"xhs","command":"node","args":["{publisher_mcp_path}"],"env":["XHS_PROFILE_DIR"]}
---

# 小红书发帖能力

通过 Publisher MCP 驱动真实浏览器完成发帖，使用确定性脚本操作，无需手动点击。浏览器 Profile 已包含登录状态。
默认会执行到最终“发布”按钮前并停住；只有 daemon 设置 `XHS_PUBLISH_DRY_RUN=0` 时才会真实点击发布。

## 前置条件

- 人类已在「连接外部账号」中通过扫码完成小红书登录，并将凭证授权给本 Agent
- `XHS_PROFILE_DIR` 会自动注入到 Publisher MCP

## 工作流程（必须遵守）

所有发布操作都需要人类审批，不得跳过：

发布小红书/抖音/快手时禁止调用 Chrome DevTools MCP；只能调用 Publisher MCP。

1. 起草内容（标题、正文、话题标签）
2. 调用 `request_approval` 提交审批
3. 等待审批（继续处理其他消息，不要阻塞）
4. 收到 "Action approved" 后调用 `publish_content` 执行发布

## 发布工具

### 发布前检查平台规范
```
get_platform_requirements(platform="xhs", content_type="image_text")
```

### 检查登录状态
```
check_login_status(platform="xhs")
```

### 发布图文
```
publish_content(
  platform="xhs",
  content_type="image_text",
  approval_action_id="<已批准的 action_id>",
  title="笔记标题",
  text="笔记正文内容",
  tags=["话题1", "话题2"],
  images=["/absolute/path/to/image1.jpg", "/absolute/path/to/image2.jpg"]
)
```

### 发布短视频
```
publish_content(
  platform="xhs",
  content_type="short_video",
  approval_action_id="<已批准的 action_id>",
  title="视频标题",
  text="视频描述",
  tags=["话题1"],
  video="/absolute/path/to/video.mp4",
  cover="/absolute/path/to/cover.jpg"  # 可选
)
```

## 内容规范

- 标题：2～20 字，简洁有吸引力
- 正文：建议 100～1000 字，可加换行和 emoji
- 图片：JPG/PNG/WebP，必须至少 1 张，建议 3:4 竖版比例，最多 9 张
- 当前自动化不支持无图文或小红书「文字配图」模式；如果用户没有提供图片，先生成或请求一张图片，并保存到 workspace 的 `artifacts/` 目录后再请求审批
- 话题标签：以列表形式传入，不含 # 号（工具自动加）；正文里不要重复手写同一批话题，工具会去重后追加缺失话题
- 视频：MP4/MOV，最长 10 分钟
- 当前自动化默认停在发布前；真实发布前确认 daemon 已设置 `XHS_PUBLISH_DRY_RUN=0`

## 注意事项

- 只能用 Publisher MCP 的 `check_login_status(platform="xhs")` 或 `publish_content` 返回结果判断发布器登录态
- 不要用 Chrome DevTools MCP 打开的页面判断小红书是否已登录；那是独立浏览器上下文，可能与 Publisher 使用的 profile 不一致
- 如 Publisher MCP 提示登录过期，通知用户重新在「连接外部账号」扫码

## 审批请求示例

```
request_approval(
  action_type="xhs_post",
  platform="xhs",
  description="发布小红书笔记：<标题>",
  payload={
    "title": "笔记标题",
    "text": "笔记正文...",
    "tags": ["话题1", "话题2"],
    "images": ["/path/to/image.jpg"]
  }
)
```

审批通过后，先调用 `execute_approved_action(action_id="...")` 读取已批准 payload，再把同一个 `action_id` 作为 `approval_action_id` 传给 `publish_content`。`publish_content` 会在发布前校验审批状态，并在发布成功后把 action 标记为已执行。
