---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node feishu webhook bridge 归档；feishu outbound 行为已迁入 adapters/messaging/feishu/（Go 实现）
replaced_by: adapters/messaging/feishu/
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/feishu-bridge/` 是 M1.0~M1.2 的飞书 outbound / 群消息 bridge，
用 Node.js 实现，通过 lightcone server 转发 channel 消息到飞书群。

M1.5 重写后：
- feishu adapter 是 Go 实现（kernel adapter framework + outbound HTTP binding）
- 路径：`adapters/messaging/feishu/`（T4 落地）
- daemon 通过 `kernel/adapter/binding.go` 的 OutboundHTTP binding 调用

旧 bridge 保留以便参考 webhook payload 格式 / 鉴权细节 / 重试策略；M2.0 启动前可删。
