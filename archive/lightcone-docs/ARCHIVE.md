---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 lightcone docs 归档；M1.5 主 spec 在 .dalek/pm/ 顶层
replaced_by: .dalek/pm/m1.5-server-rewrite-and-restructure.md / .dalek/pm/m1.5-tickets.md / .dalek/pm/v4-*
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/docs/` 是 M1.0~M1.2 期间散落的设计 / 运维 / 排错文档（包括 channel
lifecycle / device protocol / feishu bridge / pm2 部署 / sqlite vs MySQL 选型等）。

M1.5 主权威 spec 已迁到 `.dalek/pm/` 顶层：
- v4 protocol：`.dalek/pm/v4-layer{0..4}-spec.md` + `v4-message-definition.md`
- m1.5 工程纪律：`.dalek/pm/m1.5-server-rewrite-and-restructure.md` + `m1.5-tickets.md`

历史文档保留供交叉引用；M2.0 启动前可删。
