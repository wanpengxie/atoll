---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 demo 设计稿归档；M1.5 后 UI 直接由 ui/ 顶层目录承载
replaced_by: ui/（原生 UI；T7 落地）
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/demo-design/` 是 M1.0~M1.2 期间 demo UI 的设计稿（HTML mock + 静态资源），
用于内部演示 channel UI 流程。

M1.5 之后 UI 整体重写：
- 入口：`ui/index.html`
- 源码：`ui/src/*`
- 构建：vite，`make build-ui` → `ui/dist/`

设计稿保留以便回看交互逻辑参考；M2.0 启动前可删。
