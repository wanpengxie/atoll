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

# 项目指令入口

@.dalek/agent-kernel.md

# 架构硬约束（违反 = bug，PR 必拒）

<actor_envelope_invariant PRIORITY="HIGHEST">

## INVARIANT-0 — Actor / framework 数据只走 envelope

任何 actor 数据（identity / state / metadata / 历史） + framework 自身状态变更，**必须 envelope-observable**：经 `call_actor(<actor>, <reserved_type | declared_type>, ...)` 读，或经 envelope event push 到 channel log 后被订阅。

### 反射弧检查表（每次写代码前问 3 题）

1. 我要给外部暴露 actor 的什么？(identity / capabilities / live state / static metadata / 历史)
2. 能不能 `call_actor(actor, "actor.<verb>", {...})` 拿到？
   - **能** → 加 reserved type + framework intercept + SDK sugar，**server 零改**
   - **不能** → 经 envelope event push 驱动 server cache，**且必须在 actor-adapter.md 例外节登记**
3. 我是不是在"server 加 endpoint"的反射弧里？
   - **是** → **停**，回去问 1+2

### 禁止清单（PR 看到这些模式直接 request changes）

```
✗ server 直读 daemon 私有 store（channel.sqlite / actor_registry / adapter_state）
✗ server 缓存 actor metadata 第二份（除：actor.readiness.changed event push 驱动的投影）
✗ SDK 直读 channel message 流推断 actor 当前态（log 是历史，不是 truth）
✗ 加新 HTTP endpoint 前没先问"能不能 reserved type + CallActor 解决"
✗ 每个 adapter 加 `<adapter>.status` / `<adapter>.describe` per-type convention
✗ adapter Module 状态变更不 emit envelope event（直写 sqlite 替代）
```

### 正确模式

```
✓ kernel/adapter                     新 reserved type 名加到 reservedActorTypeSet
✓ adapters/framework/manager.go      respondActor<X> framework intercept
✓ pkg/coagentsdk/client.go           Client.<X> = sugar over CallActor(reserved_type)
✓ server/                            零改动
```

### 完整 spec

`.dalek/pm/actor-adapter.md` 顶部 `INVARIANT-0` 节 + §26 不变量表 + §27 口号。

### 不变量 lineage

- **Unix /proc 模式**：kernel 状态 reify 成 file，但 syscall 不强塞文件抽象
- **Slack events API 模式**：admin 操作经独立 API，但状态变更必须 emit observable event
- coagent 同型：**写侧（composition root install）保留 Go method**；**读侧 + 状态变更 audit 通过 envelope**

### 此 PM 已经犯过的错（防再犯）

| 错误模式 | 反射弧 |
|---|---|
| 读 sqlite 拿 device 状态 | 缺方法 → 直插 store |
| SDK 加 `ListMessages` 读 log 拿状态 | 缺 read API → 把 log 当 state 源 |
| SDK 加 `ActorStatus` 走 server devicebus connection 表 | 缺接口 → server 加端点 |
| 每个 adapter 加 `<x>.status` type | 缺 substrate-level → 降级 per-adapter |
| `system.adapter.declared` event + server viewcache parse | 想 server 缓存第二份 |
| `server/actorbus/` 直读 channel.sqlite | server bypass envelope 直接读 daemon store |

**再撞这些反射弧，先停下念 3 题检查表**。

</actor_envelope_invariant>

<ownership_invariants PRIORITY="HIGHEST">

## INVARIANT-1 ~ 6 — 6 个 ownership invariants（`.go-arch-lint.yml` 硬卡）

源：`.dalek/pm/coagent-architecture.md` §0。**`make lint` fail = bug**。

| # | Invariant | 实质 |
|---|---|---|
| 1 | **kernel 顶层一等公民** | `kernel/**` 不可 import 任何具体后端（sqlite / HTTP / gorilla / gin / sqlite drivers / go-kimi）。纯协议契约。 |
| 2 | **truth ownership** | channel 协作真相归 daemon-side（`runtime/store`），server 仅持 view cache。 |
| 3 | **directory ownership** | `kernel/` 是协议根，`runtime/` `server/` `adapters/` `cmd/` 都只能向下依赖 kernel。 |
| 4 | **session ownership** | worker 绑 `daemon_epoch + fencing_token`，daemon 是唯一 session/lease 仲裁者。 |
| 5 | **adapter ownership** | `adapters/` 只通过 framework 与 kernel 接触，不能反向 import `runtime/` `server/`。 |
| 6 | **worker ownership** | worker 不直接写 daemon sqlite，所有 mutation 走 IPC → harness。 |

依赖方向单向：
```
cmd/** ─→ runtime/** + server/** + adapters/** ─→ kernel/**
```

</ownership_invariants>

<protocol_closed_sets PRIORITY="HIGHEST">

## INVARIANT-7 ~ 10 — 协议级闭集（frozen）

改任何一条 = 协议级修订，往 `proto-foundation` / `proto-layer0` / `proto-layer1` 提，不在 impl 层动。

| # | 闭集 | 项目 | 源 |
|---|---|---|---|
| 7 | **envelope 17 字段** | id / channel_id / kind / type / sender / audience / parent_id / correlation_id / payload / ts / ts_received / expires_at / visibility / seq / is_terminal / ... | proto-foundation §1.1 |
| 8 | **actor.kind 4 项** | `human` / `agent` / `system` / `tool` | proto-foundation §2.6 |
| 9 | **binding 3 项** | `embedded` / `runtime_outbound` / `runtime_inbound_via_relay` | proto-foundation §2.5.1 |
| 10 | **terminal reason 3 项** | `unanswered_timeout` / `receiver_internal_error` / `receiver_unavailable` | proto-layer0 §2.6 |

</protocol_closed_sets>

<runtime_invariants PRIORITY="HIGHEST">

## INVARIANT-11 ~ 14 — runtime 行为

| # | Invariant | 守门人 |
|---|---|---|
| 11 | **Closure**：每条 audience 点到 actor 的 request envelope，最终必有 terminal response（自答 or substrate F3 timer 兜底） | harness Step 8 + framework F3 |
| 12 | **Append-only log**：channel log 只追加，不回写历史；状态由 event 投影 | runtime/store messages 表 schema |
| 13 | **Harness 9 步必走**：所有 envelope write 走 `kernel/harness.Chain.Write`（runtime/harness 实现），不绕过 | proto-layer1 §2.5 + runtime/harness |
| 14 | **时延上界 R5**：SDK 默认 30s / max_pending_ms 默认 30s；长跑业务 type 显式覆盖；离线 adapter 调用 ms-秒级 fail，不等 F3 5min | actor-adapter.md §7.2 R5 + dispatch pre-check |

</runtime_invariants>

<pm_invariants PRIORITY="HIGHEST">

## INVARIANT-15 ~ 16 — PM 自身

来自 `.dalek/agent-kernel.md` invariants 节：

| # | Invariant | 守门人 |
|---|---|---|
| 15 | **PM 不直接修改产品实现文件**（`kernel/` `runtime/` `server/` `adapters/` `cmd/` `ui/` 测试文件、前端资源） | dalek invariant #10 |
| 16 | **PM merge 冲突涉及产品文件**：abort + 创建 integration ticket，**不手工解冲突** | dalek invariant #11 |

**例外**：owner 显式说 "你直接写" / "就你自己直接写" 时 PM 可临时直改，但事后必须在 commit message 中明示授权。

</pm_invariants>

<banned PRIORITY="HIGHEST">

## INVARIANT-17 — 零 MCP

来自 `.dalek/agent-kernel.md` `<banned>` 节（owner 决策 2026-04-29）：

整个 coagent 项目**禁止使用和创建任何 MCP**：
- `kernel/` `runtime/` `server/` `adapters/` `cmd/` `pkg/` 禁止创建 MCP server / 引用 `@modelcontextprotocol/sdk` / 注入 MCP server / 设计 "内核 MCP" 或 "业务 MCP" 概念
- 所有合法动作集统一走 CLI binary / IPC 边界

违反 = bug。任何 worker / PM / spec 出现 "MCP server"、"capability_set.mcp_servers" 等表述都立即拒绝并返工。

</banned>

<invariant_index>

## 完整索引

| 簇 | 数 | 源 |
|---|---|---|
| INVARIANT-0 actor envelope | 1 | 本文上方 + `actor-adapter.md` 顶部 |
| INVARIANT-1~6 ownership | 6 | `coagent-architecture.md` §0 + `.go-arch-lint.yml` |
| INVARIANT-7~10 protocol 闭集 | 4 | `proto-foundation` / `proto-layer0` |
| INVARIANT-11~14 runtime 行为 | 4 | `proto-layer1` / `actor-adapter.md` §26 |
| INVARIANT-15~16 PM 自身 | 2 | `agent-kernel.md` |
| INVARIANT-17 零 MCP | 1 | `agent-kernel.md` `<banned>` |

**总 18 条**。每次准备做"看起来需要绕过抽象"的事，先扫一遍这表，再决定。

</invariant_index>
