---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node 双栈归档；agent demo binary 已被新 4 binary 栈（cmd/{server,daemon,worker,cli}）取代
replaced_by: cmd/coagent-worker（Go 实现）
safe_to_delete_after: M2.0 启动前
---

历史背景：

`agent-binary/` 是 M1.0~M1.2 期间用 TypeScript 实现的 single-agent demo runner，
被 lightcone Node daemon 调度起来跑 channel 内的 agent loop。

M1.5 重写后 agent runner 整体迁到 Go：
- 入口：`cmd/worker/main.go`
- runtime：`runtime/worker/`
- IPC：`runtime/ipc/`
- daemon 侧 host：`runtime/workerhost/`

本目录保留供 M1.3-M1.4 spec / 评审记录交叉引用。
