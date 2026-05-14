---
archived_at: 2026-05-14
reason: M1.5-T9 — channel workspace 模板目录归档；新栈 channel workspace 改为 daemon 直接管 (runtime/store / runtime/bootstrap)
replaced_by: runtime/bootstrap/ + runtime/store/（运行时直接创建 channel workdir）
safe_to_delete_after: M2.0 启动前
---

历史背景：

`workspace-template/` 是 M1.0~M1.2 的 channel workspace 初始化模板，被旧 lightcone
daemon 用 `cp -r workspace-template channels/<channel-id>` 的方式实例化，包含：
- `agents/`：channel agent 子目录骨架（包含 system prompt / capability 等）
- `messages/`：channel message 持久化骨架
- `artifacts/`：channel 生成产物骨架
- `schedules/`：channel 内定时任务骨架
- `PROJECT.md.tpl`：channel 元数据模板

M1.5 后 channel workspace 由 daemon Go 代码直接管理：
- `runtime/bootstrap/`：channel lifecycle 起点
- `runtime/store/`：channel state 持久化（sqlite，本地 daemon-local）
- `runtime/scheduler/`：channel 内 actor 调度
- channel workdir 不再使用文件系统模板复制；目录骨架在代码里现造

模板保留以便回看 channel 目录约定 / 文件命名；M2.0 启动前可删。
