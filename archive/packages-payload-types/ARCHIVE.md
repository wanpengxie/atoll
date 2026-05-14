---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node 共享 payload TS 类型包归档；v4 协议合同由 kernel/ Go 类型 + pkg/v4types 承载
replaced_by: kernel/message/ + pkg/v4types/
safe_to_delete_after: M2.0 启动前
---

历史背景：

`packages/payload-types/` 是 M1.0~M1.2 期间在 Node 双栈（lightcone server + agent
binary + Node daemon + cli）之间共享的 TypeScript payload type 定义包，作为
pnpm workspace 包发布。

M1.5 重写后 v4 协议合同：
- 权威类型：`kernel/message/`（Go），由 `.go-arch-lint.yaml` 保护 ownership
- 旧 v0~v3 互操作类型（grandfather 行为）：`pkg/v4types/`
- TypeScript 侧（UI / Chrome 扩展）目前不依赖共享类型包，必要时由 ui/ 或
  extension 自带子集类型

旧包保留以便参考类型形状演化历史；M2.0 启动前可删。
