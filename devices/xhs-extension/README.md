# Coagent · 小红书 Device

Coagent 的 chrome 端 device 实现：长连 coagent daemon 的 `/device/{deviceId}` WebSocket，
承载 daemon 派发的 5 个小红书业务命令（publish / search / get-my-recent / get-note /
publish-status），并把 chrome.cookies 主动同步给 daemon 用作 session。

> 来源：本目录 M1.1-T2 一次性 rsync 自 1studio extension（蓝本：`~/tardis/1studio/extension/`），
> 之后在 coagent 仓库独立维护，**不再 sync 1studio 母本**。详见
> `.dalek/pm/m1.1-xhs-real-onboarding-spec.md` §六。

## 架构

```
agent (channel-agent)
  ↓ SDK Bash
cli/bin/xhs (shim) → coagent-xhs (Go binary, T1)
  ↓ daemon HTTP /rpc device.command.send
coagent daemon (T3)
  ↓ DeviceWsServer.pushCommand
  WS /device/{deviceId}?key=...
本插件 (T2)
  ↓ services/coagent-device.ts dispatch → tools/*.ts handler
xhs.com / creator.xiaohongshu.com（用户实操或 DOM/CDP 抓取）
  ↓ POST {daemonHttpBase}/api/device/{deviceId}/callback
coagent daemon → emit dispatch.completed → trigger gateway → REACT → wake agent
```

## 开发 / 构建

```bash
cd devices/xhs-extension
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

打开扩展 popup，填以下 5 字段并点 "连接"：

| 字段 | 说明 | 示例 |
|---|---|---|
| Daemon WebSocket URL | 长连地址；若不带 path，自动追加 `/device/{deviceId}` | `ws://127.0.0.1:9501` 或 `ws://host:9501/device/xhs-laptop-001` |
| Daemon HTTP base | callback / session POST 的根 URL | `http://127.0.0.1:9501` |
| Device ID | 与 daemon `DEVICE_KEYS` 中的 deviceId 一致 | `xhs-laptop-001` |
| Device API Key | 与 daemon `DEVICE_KEYS` 中对应 device 的 key 一致 | `sk_dev_xxx` |
| 主人 user_id（可选） | 写入 session sync 的 user_id | `user-001` |

配置写入 `chrome.storage.local`（key=`coagent_device_config`）。Service Worker 启动时
若 4 个核心字段（serverUrl/apiKey/deviceId 任一为空除外）齐全且自动重连开启，会自动连接。

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

**connect 报 "Device 配置不完整"** — 5 字段中 daemon WS URL / device api key / device id 任一为空。检查 popup。

**callback 返回非 2xx** — daemon 校验 device_api_key 失败或 correlation_id 已过期；service worker console 会打印 `callback non-2xx` 含 status 与 body。

**publish 调起后没有动作** — 检查是否在 xhs.com 已登录；publish handler 不会自己代登录，需要用户已登录 xhs。

## 许可证

继承自 1studio 母本：MIT。
