<!-- DALEK:INJECT:BEGIN -->
<dalek_bootstrap PRIORITY="HIGHEST" override="true">

<definition>
  dalek agent bootstrap 区。负责dalek的启动引导。
  本分区是启动硬约束，优先级最高。若与本文件其他内容冲突，以本分区为准。
</definition>

<state_assumption>
  本项目是dalek持续维护和开发的workspace
  当前项目的历史运行态（tickets、workers、worktrees）
</state_assumption>

<bootstrap_process CRITICAL="true">
  <!-- MUST: 首次回复或执行任何动作前，必须完成以下启动序列 -->
  <step_1>
    读取dalek操作系统内核：位于.dalek/agent-kernel.md —— 这是dalek的操作指令和状态空间
    读取 `bootstrap_token`，记住此值，用于校验。
  </step_1>
  <hard_rule MUST="true">
    禁止在完成“全量逐行读取”之前，通过搜索或者检索的方式读取bootstrap_token
    禁止使用 grep/rg/awk/sed 提取 bootstrap_token
    违者视为 bootstrap 失败。
  </hard_rule>
</bootstrap_process>

<bootstrap_verify MUST="true">
  Bootstrap 校验 —— MUST 执行以下动作：
  <kernel_verify MUST="true">
  echo "BOOTSTRAP_OK:{token值}"
  这是强制动作，不可跳过，不可用文字替代。必须产生真实的 Bash tool call。
  </kernel_verify>
</bootstrap_verify>

<!-- 重申（第二遍）：在你做任何事之前，先读 .dalek/agent-kernel.md。这不是建议，是硬性要求。 -->
<!-- 注意：你不是在开发dalek项目，你是在开发当前这个git repo，dalek是你自己的身份 -->

</dalek_bootstrap>
<!-- DALEK:INJECT:END -->

# Repository Guidelines

## Project Structure & Module Organization
`src/` contains the main lightcone server. Keep HTTP routes in `src/routes/`, persistence in `src/db/`, realtime delivery in `src/realtime/`, daemon socket handling in `src/daemon/`, and scheduled agent work in `src/scheduler/`. Frontend assets are served from `public/`; `public/generated/` is only for temporary public upload URLs, not durable agent deliverables.

This repo also includes companion Node services: `daemon/` for machine-side agent execution, `feishu-bridge/` for Feishu integration, and `mcp-servers/mysql/` for the MySQL MCP server. Add new modules next to the closest existing domain file instead of creating broad utility folders.

## Build, Test, and Development Commands
Install dependencies from each package root with `npm install`.

- `npm run dev` - start the main server with file watching.
- `npm start` - run the main server once.
- `cd daemon && npm run dev` - run the daemon locally.
- `cd feishu-bridge && npm run dev` - run the Feishu bridge locally.

There is no checked-in `test` or `lint` script yet. For changes to API or agent flows, verify manually by running the affected service and exercising the relevant route, socket event, or bridge behavior.

## Coding Style & Naming Conventions
The codebase uses ES modules, semicolons, and relative imports with explicit `.js` extensions. Follow the existing style: 2-space indentation inside blocks, `camelCase` for variables/functions, `UPPER_SNAKE_CASE` for env-backed constants, and kebab-free file names grouped by feature such as `src/routes/messages.js` or `src/xhs/account-manager.js`.

Prefer small modules with one responsibility. Keep route handlers thin and push storage logic into `src/db/`.

## Testing Guidelines
No first-party automated tests are present today. When adding tests, place them outside `node_modules/` in a dedicated `test/` or package-local `__tests__/` directory and name files `*.test.js`. Prioritize route coverage, DB edge cases, and daemon/bridge message handling. Document the manual checks you ran in the PR until a shared test runner is added.

## Commit & Pull Request Guidelines
Recent commits use short imperative subjects, for example `Add Feishu bridge for multi-agent group chat integration`. Keep commits focused and descriptive.

Pull requests should include a concise summary, linked issue or task when available, manual verification steps, and screenshots for `public/` UI changes. Call out schema, env, or port changes explicitly and update the relevant `.env.example` file in the same PR.
@.dalek/agent-kernel.md
