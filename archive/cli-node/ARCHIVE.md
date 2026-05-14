---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node CLI 归档；`coagent` 命令现由 Go 实现
replaced_by: cmd/cli/（Go 实现，构建为 bin/coagent-cli）
safe_to_delete_after: M2.0 启动前
---

历史背景：

`cli/`（原 `xhs-cli` / `coagent` Node 包）是 M1.0~M1.2 的运维 + 业务 CLI 入口，
负责 channel / message / xhs publish / admin 等子命令，通过本地 socket 与 lightcone
Node daemon 通信。

M1.5 重写后 CLI 整体迁到 Go：
- 入口：`cmd/cli/main.go`
- 子命令：channel / message / xhs publish / admin（与旧 CLI 行为对齐）
- 构建产物：`bin/coagent-cli`（make build）

旧 Node CLI 保留以便参考子命令交互细节；M2.0 启动前可删。
