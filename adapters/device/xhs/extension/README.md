# Coagent · 小红书 Device

Coagent 的 chrome 端 device 实现。承载 5 个小红书业务命令
（publish / search / recent.fetch / note.fetch / cookie.sync）。

> M1.5-T5：本目录从 `devices/xhs-extension` 重组到
> `adapters/device/xhs/extension`（go-arch-lint adapters/ 顶层化的一部分）。
> 协议从 daemon WS 直连（M1.3 baseline）切到 via_server_transit binding
> （连 server WS，承载 daemon ↔ adapter 的 device_transit 帧）—— 协议层
> 切换的完整落地等 T6 server.devicebus + T7 cmd/* 接入；T5 阶段保留旧
> WS 客户端代码，server WS / token 接入分阶段添加，参见 `services/`。
>
> 来源历史：M1.1-T2 一次性 rsync 自外部模板，之后在 coagent 仓库独立
> 维护。

## 架构（M1.5 via_server_transit 目标态）

```
agent (channel-agent)
  ↓ SDK Bash / 直接 envelope
adapters/device/xhs (Go, M1.5-T5)
  ↓ kernel/adapter.DeviceTransit.Send(device_transit.send frame)
runtime/transit (M1.5-T3) → daemonbus mux → server.devicebus (M1.5-T6)
  ↓ device WS endpoint
本插件 (M1.5-T5)
  ↓ services/coagent-device.ts dispatch → tools/*.ts handler
xhs.com / creator.xiaohongshu.com
  ↓ device_transit.recv frame
server.devicebus → daemonbus → runtime/transit → adapter.OnExternalCallback
  ↓ ctx.Respond (sender=tool:xhs-adapter)
channel log（device 不出现在 actor_registry — L4 §2.6）
```

## 开发 / 构建

```bash
cd adapters/device/xhs/extension
pnpm install               # 第一次运行需要拉依赖
pnpm dev                   # WXT dev 模式（实时重载）
pnpm build                 # 生产构建：app/chrome-extension/.output/chrome-mv3/
```

`pnpm install` 期间会先构建 `packages/shared`（tsup），再链接到 `app/chrome-extension`。

## 加载到 Chrome（load unpacked）

1. 打开 `chrome://extensions/`
2. 启用 "开发者模式"
3. 点 "加载已解压的扩展程序"
4. 选择 `app/chrome-extension/.output/chrome-mv3/` 目录（或 `chrome-mv3-dev/`）

## Daemon 配置

### 主流程：1 个 api-key（推荐）

打开扩展 popup，主入口只填 2 项：

| 字段 | 说明 | 示例 |
|---|---|---|
| Coagent api-key | 在 coagent server 创建 device 时分配的 key | `sk_dev_xxx` |
| Server URL | coagent server 部署地址；popup 默认 `https://coagent-server`，必须改成真实部署域名 | `https://coagent.example.com` |

点 "连接" 后扩展走 `POST {Server URL}/api/device/resolve {api_key}`，
反查到对应 daemon 的 `ws_url / http_url / device_id / user_id / channel_id /
daemon_id`，落 `chrome.storage.local`，然后用 daemon WS URL 建立长连。

resolve 失败（网络 / 401 / 404 / 429 / server 5xx）会在 popup 内显示中文友好
错误，并提示「若 server 不可达，可展开 Advanced 直接配 daemon 5 字段」。

### Advanced 折叠：旧 5 字段（dev / test）

popup 下方 "⚙️ Advanced" 折叠区保留旧 5 字段，跳过 coagent resolve 直接连
daemon。多用于 coagent server 暂不可用、本地起 daemon 调试等场景：

| 字段 | 说明 | 示例 |
|---|---|---|
| Daemon WebSocket URL | 长连地址；若不带 path，自动追加 `/device/{deviceId}` | `ws://127.0.0.1:9501` 或 `ws://host:9501/device/xhs-laptop-001` |
| Daemon HTTP base | callback / session POST 的根 URL | `http://127.0.0.1:9501` |
| Device ID | 与 daemon `DEVICE_KEYS` 中的 deviceId 一致 | `xhs-laptop-001` |
| Device API Key | 与 daemon `DEVICE_KEYS` 中对应 device 的 key 一致 | `sk_dev_xxx` |
| 主人 user_id（可选） | 写入 session sync 的 user_id | `user-001` |

### Storage shape

配置写入 `chrome.storage.local`（key = `coagent_device_config`）：

```jsonc
{
  // 主入口字段
  "coagentServerUrl": "https://coagent.example.com",

  // Daemon 连接字段（resolve 自动填，或 advanced 手动填）
  // serverUrl / wsUrl 为同一个值的 canonical+alias；daemonHttpBase / httpBase 同理
  "serverUrl":      "ws://daemon-host:9501/device/xhs-laptop-001",
  "wsUrl":          "ws://daemon-host:9501/device/xhs-laptop-001",
  "daemonHttpBase": "http://daemon-host:9501",
  "httpBase":       "http://daemon-host:9501",

  "deviceId":  "xhs-laptop-001",
  "apiKey":    "sk_dev_xxx",
  "userId":    "user-001",

  // Resolve 元数据（仅 main 入口流程会填，备用）
  "channelId": "ch-1",
  "daemonId":  "daemon-1",

  "autoReconnect": true
}
```

Service Worker 启动时若 `serverUrl/apiKey/deviceId` 三项齐全且 `autoReconnect`
为 true，会自动连接。旧版本（仅含 `serverUrl/daemonHttpBase/apiKey/deviceId/
userId/autoReconnect` 的 advanced shape）天然兼容，加载时新增字段为空不影响。

## 命令协议（spec §4）

入站 frame（daemon → ext）：
```jsonc
{
  "type": "command",
  "correlation_id": "01HXX...",
  "cmd": "publish",                  // publish | search | get-my-recent | get-note | publish-status
  "params": { /* 命令参数 */ },
  "session": { "cookies": [...], "user_id": "..." }
}
```

完成回调（ext → daemon HTTP）：
```
POST {DAEMON_HTTP}/api/device/{deviceId}/callback
Authorization: Bearer {device_api_key}
Content-Type: application/json
{
  "correlation_id": "01HXX...",
  "status": "ok" | "error",
  "result": {...} | null,
  "error": {"code": "...", "message": "..."} | null
}
```

cookie 同步（spec §6.2.4）：
```
POST {DAEMON_HTTP}/api/device/{deviceId}/session
Authorization: Bearer {device_api_key}
Content-Type: application/json
{ "user_id": "user-001", "cookies": [...], "login_state": "logged_in" | "unknown" }
```

## 5 个命令

| cmd | handler | 说明 |
|---|---|---|
| `publish` | `tools/publish-content.ts` | 跳到发布页 → DOM/CDP 输入 → 用户确认发布；返回 `{success, manualPublishPending, ...}` |
| `search` | `tools/search-feeds.ts` | 搜索小红书 feed，返回 feeds 数组 |
| `get-my-recent` | `tools/xiaohongshu/get-my-recent.ts`（衍生自 analyze-my-profile） | 当前登录账号最近 N 条笔记 |
| `get-note` | `tools/xiaohongshu/get-note.ts`（新增） | 拿单条 note 详情；需要 url 含 xsec_token 或显式传 xsec_token |
| `publish-status` | `tools/xiaohongshu/publish-status.ts`（新增，包 get-note-analytics） | `published` / `not_found` / `unknown` |

## 调试

- Service Worker 日志：`chrome://extensions/` → 找到扩展 → "Service Worker" → console
- popup 日志：右键扩展图标 → "审查弹出窗口" → console
- 长连断开：`chrome.storage.local.get('coagent_device_status')` 看快照；popup 上方状态指示器同步
- Cookie sync：成功后 daemon `<data>/users/{user_id}/xhs-session.json` 会被原子覆盖

## 常见问题

**connect 报 "Device 配置不完整"** — daemon WS URL / device api key / device id 任一为空。检查 popup（主入口走 resolve 自动填；advanced 需要全填）。

**主入口 resolve 报 "Server 不可达 / 超时"** — 检查 Server URL 是否填的真实 coagent 部署域名（默认 `https://coagent-server` 是 placeholder）。临时绕过：展开 Advanced，手动填 daemon 5 字段。

**callback 返回非 2xx** — daemon 校验 device_api_key 失败或 correlation_id 已过期；service worker console 会打印 `callback non-2xx` 含 status 与 body。

**publish 调起后没有动作** — 检查是否在 xhs.com 已登录；publish handler 不会自己代登录，需要用户已登录 xhs。

## 许可证

MIT。
