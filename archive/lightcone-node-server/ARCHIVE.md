---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node lightcone server 归档；功能全部迁到 cmd/server Go 实现
replaced_by: cmd/server/（Go 实现，构建为 bin/coagent-server）+ server/*
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/src/`（原 `lightcone/lightcone-server`）是 M1.0~M1.2 的核心 Node.js
服务端，承担：
- HTTP API（用户 / channel / device / message / admin）
- WebSocket gateway（push 推流）
- MySQL schema 管理（initDb 自启时刷 schema）
- daemon 注册 + 心跳 + control message 转发
- device resolve API（xhs extension 反查 daemon 连接信息）

M1.5 整套搬到 Go 单 binary：
- gateway：`server/gateway/`
- daemon control bus：`server/daemonbus/`
- device control bus：`server/devicebus/`
- placements + channel state machine：`server/placements/`
- catalog / identity / pushhub：对应 `server/*`
- store：sqlite 替换 MySQL，`server/store/migrations/*`
- 入口：`cmd/server/main.go`

旧 Node server 保留以便参考：
- API 合同细节（query/body 字段、状态码、error code）
- `routes/device.js` 的 resolve 流程（xhs extension 仍然兼容 resolve 协议）
- DB schema 演进历史
- push 推流的 envelope 格式（v0~v3 演化）

M2.0 启动前可删。
