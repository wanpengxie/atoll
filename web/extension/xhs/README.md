# Coagent · 小红书 Device

Coagent 的 Chrome 端 xhs device 实现，承载 publish / search / recent.fetch /
note.fetch / cookie.sync 等小红书业务命令。

当前连接模型是 coagent-proxy：

```
agent (channel-agent)
  -> adapters/device/xhs
  -> runtime/transit -> daemonbus -> server.devicebus v2
  -> coagent-proxy local endpoint
  -> extension services/server-devicebus.ts
  -> tools/*.ts
  -> xhs.com / creator.xiaohongshu.com
```

Extension 只连接本机 proxy endpoint。旧的 server 下发 actor token / 直连
server devicebus / resolve fallback 流程已下线。

## 开发 / 构建

```bash
cd adapters/device/xhs/extension
pnpm install
pnpm dev
pnpm build
```

生产构建输出在 `app/chrome-extension/.output/chrome-mv3/`。

## 加载到 Chrome

1. 打开 `chrome://extensions/`
2. 启用 "开发者模式"
3. 点 "加载已解压的扩展程序"
4. 选择 `app/chrome-extension/.output/chrome-mv3/` 或 dev 输出目录

## Proxy 配置

1. 用户在 server UI 为 channel 创建 proxy daemon 并拿到 daemon api key。
2. 启动 `coagent-proxy`，由 proxy daemon 连接 server `/devicebus/v2/connect`。
3. 在 extension popup 输入本机 proxy endpoint，默认 `ws://127.0.0.1:10387`。
4. Extension 连接本机 endpoint 并发送 hello 选择 `tool:xhs`。

Storage 写入 `chrome.storage.local`，key 为 `coagent_device_config`：

```jsonc
{
  "connectionMode": "proxy",
  "proxyEndpoint": "ws://127.0.0.1:10387",
  "deviceActorId": "tool:xhs",
  "autoReconnect": true
}
```

Service Worker 启动时若 `connectionMode=proxy` 且 `autoReconnect=true`，会自动
连接本机 proxy endpoint。

## 命令协议

入站 frame：

```jsonc
{
  "type": "command",
  "correlation_id": "01HXX...",
  "cmd": "publish",
  "params": {},
  "session": { "cookies": [], "user_id": "..." }
}
```

完成回调通过同一条 proxy WS 返回：

```jsonc
{
  "direction": "from_device",
  "actor_id": "tool:xhs",
  "correlation_id": "01HXX...",
  "payload": {
    "correlation_id": "01HXX...",
    "status": "ok",
    "result": {},
    "error": null
  }
}
```

## 调试

- Service Worker 日志：`chrome://extensions/` -> 扩展 -> "Service Worker"
- popup 日志：右键扩展图标 -> "审查弹出窗口"
- 连接快照：`chrome.storage.local.get('coagent_device_status')`

常见连接失败优先检查本机 `coagent-proxy` 是否已启动，以及 popup endpoint 与
proxy 监听端口是否一致。
