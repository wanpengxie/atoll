---
archived_at: 2026-05-14
reason: M1.5-T9 — lightcone/ 顶层杂项归档（demo public assets、test、tuoguan 自托管脚本、AGENTS/README、.env.example、.gitignore、package-lock.json、历史 .dalek/pm 笔记）
replaced_by: 零散文件按用途散归到对应位置（详见下）
safe_to_delete_after: M2.0 启动前
---

历史背景：

本目录收纳 `lightcone/` 顶层杂项文件，按子目录组织：

- `public/`：M1.0~M1.2 demo HTML 入口（demo.html / devices.html / index.html /
  job-query.html / todo.html / tasks-fixture.js / generated/）。T7 已用全新
  `ui/` 替换；这些 HTML demo 文件未被 ui/ 复用，保留以便回看交互逻辑。
- `test/`：旧 lightcone-server 集成测试集合。
- `dalek-pm/`：原 `lightcone/.dalek/pm/` 下的 5 篇 redesign / orchestrator 笔记，
  属于 lightcone 内部讨论，未升级为顶层 spec。
- `tuoguan.sh / tuoguan.log / .tuoguan.pid`：M1.0~M1.2 单机自托管启动脚本 +
  历史日志 + 残留 pidfile。
- `AGENTS.md / CLAUDE.md`：lightcone 子目录原 agent 提示文档（覆盖在 lightcone/
  下工作的引导）。
- `README.md`：lightcone 子目录原 README。
- `.env.example`：lightcone 老 server 环境变量样例（基于 MySQL）。
- `.gitignore`：lightcone 子目录历史 gitignore。
- `package-lock.json`：lightcone 老 Node 包锁文件（package.json 已删除）。

M2.0 启动前可删。
