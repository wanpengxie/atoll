---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node daemon 归档；M1.5 重写为 Go binary
replaced_by: cmd/daemon/（Go 实现，构建为 bin/coagent-daemon）
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/daemon/` 是 M1.0~M1.2 的 Node.js 单机 daemon，负责：
- channel 进程编排（fork / supervise / restart）
- xhs device 注册 + 反向连接
- feishu outbound webhook
- HTTP + WebSocket 服务

M1.5 整套搬到 Go：
- daemon 入口：`cmd/daemon/main.go`
- supervisor + scheduler：`runtime/supervisor/` `runtime/scheduler/`
- bootstrap：`runtime/bootstrap/`
- store：`runtime/store/`

旧 daemon 保留以便参考 channel lifecycle / device resolve / feishu webhook 等行为细节；
M2.0 启动前可删。
