---
archived_at: 2026-05-14
reason: M1.5-T9 — 旧 Node ops 脚本归档；新栈使用 make + Go binary，无对应 doctor / register-machine 等价物
replaced_by: make build / make migrate / `bin/coagent-cli admin status` / `bin/coagent-cli admin machines`
safe_to_delete_after: M2.0 启动前
---

历史背景：

`ops/*.mjs`（Node ESM 单文件脚本）是 M1.0~M1.2 的运维入口：

- `doctor.mjs`：综合自检（PATH / .env / MySQL 连通 / schema 漂移检测 /
  pm2 状态 / daemon 健康 / 文件权限 / 项目 runtime 文件），及其单元测试
  `doctor.test.mjs`；依赖 `schema-sentinel.json` 维护预期表清单。
- `register-machine.mjs`：本地 machine 注册到 lightcone server，写
  `~/.coagent/<project_key>/machine.key`；及其单元测试 `register-machine.test.mjs`。
- `schema-sentinel.json`：doctor 用的 MySQL 预期表清单。
- `env.example`：旧 quickstart 引用的 .env 样例（MySQL / ANTHROPIC_API_KEY /
  SERVER_URL / ADMIN_TOKEN / pm2 等），M1.5 sqlite 单 binary 栈不再需要。

M1.5 之后：
- 数据库换 sqlite，daemon-local + server-local 各自管 schema migration（`make
  migrate` 走 golang-migrate）
- 健康检查改为 daemon `/health` + server `/health` HTTP endpoint
- machine 注册改为 daemon 启动时 register 到 server（HTTP 接口）
- pm2 编排改为直接 systemd unit / supervisor / bare exec（demo 期最简）

旧脚本保留以便回看自检项 / 注册流程 / schema 演化清单；M2.0 启动前可删。

注意：spec §T9 列了 `ops/smoke-channel-runtime.mjs`，但 HEAD=1fe4e57 上该文件已
不存在（推断更早 ticket 已删/未在分支建过），故未归档。
