---
name: kuaishou-post
description: 在快手发布图文或短视频内容。所有发布操作需经人类审批。
tags: ["kuaishou", "publishing", "social-media", "browser", "video"]
mcp_config: {"server":"publisher","platform":"kuaishou","command":"node","args":["{publisher_mcp_path}"],"env":["KUAISHOU_PROFILE_DIR"]}
---

# 快手发帖能力

通过 Publisher MCP 驱动真实浏览器在快手创作者平台完成发帖。使用确定性脚本操作，无需手动点击。

## 前置条件

- 人类已在「连接外部账号」中通过扫码完成快手登录，并将凭证授权给本 Agent
- `KUAISHOU_PROFILE_DIR` 会自动注入到 Publisher MCP

## 工作流程（必须遵守）

所有发布操作都需要人类审批，不得跳过：

发布小红书/抖音/快手时禁止调用 Chrome DevTools MCP；只能调用 Publisher MCP。

1. 准备内容（文案、话题标签、媒体文件路径）
2. 调用 `request_approval` 提交审批
3. 等待审批（继续处理其他消息，不要阻塞）
4. 收到 "Action approved" 后调用 `publish_content` 执行发布

## 发布工具

### 发布前检查平台规范
```
get_platform_requirements(platform="kuaishou", content_type="image_text")
get_platform_requirements(platform="kuaishou", content_type="short_video")
```

### 检查登录状态
```
check_login_status(platform="kuaishou")
```

### 发布图文
```
publish_content(
  platform="kuaishou",
  content_type="image_text",
  approval_action_id="<已批准的 action_id>",
  title="图文标题",
  text="图文描述内容",
  tags=["话题1", "话题2"],
  images=["/absolute/path/to/image1.jpg"]
)
```

### 发布短视频
```
publish_content(
  platform="kuaishou",
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

- 标题：简洁明了
- 正文：最多 1000 字
- 图片：最多 12 张，JPG/PNG
- 视频：MP4/MOV，最长 10 分钟，建议 9:16 竖版
- 话题标签：以列表形式传入，不含 # 号；正文里不要重复手写同一批话题，工具会去重后追加缺失话题

## 注意事项

- 视频上传时间较长，上传完成前不要关闭浏览器
- 只能用 Publisher MCP 的 `check_login_status(platform="kuaishou")` 或 `publish_content` 返回结果判断发布器登录态
- 不要用 Chrome DevTools MCP 打开的页面判断快手是否已登录；那是独立浏览器上下文，可能与 Publisher 使用的 profile 不一致
- 如 Publisher MCP 提示登录过期，通知用户重新在「连接外部账号」扫码

## 审批请求示例

```
request_approval(
  action_type="kuaishou_post",
  platform="kuaishou",
  description="发布快手视频：<标题>",
  payload={
    "title": "视频标题",
    "text": "视频描述...",
    "tags": ["话题1"],
    "video": "/path/to/video.mp4"
  }
)
```

审批通过后，先调用 `execute_approved_action(action_id="...")`，再把同一个 `action_id` 作为 `approval_action_id` 传给 `publish_content`。
