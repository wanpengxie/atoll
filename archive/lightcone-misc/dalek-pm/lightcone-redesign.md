# lightcone 架构重构设计

> 状态：设计 v1，未进入开发计划
> 最后更新：2026-04-20

## 文档索引

| 文档 | 内容 |
|------|------|
| [lightcone-redesign.md](./lightcone-redesign.md)（本文） | 架构总览：Context Governance、Goal State、Orchestrator / Worker、前端、托管 runtime |
| [lightcone-redesign-schema.md](./lightcone-redesign-schema.md) | 数据库表结构：Context Item、Goal State、Bundle、Eval 等 |
| [lightcone-redesign-open-questions.md](./lightcone-redesign-open-questions.md) | 设计问题清单与逐项结论 |
| [orchestrator-system-prompt.md](./orchestrator-system-prompt.md) | Orchestrator context-first system prompt |
| [lightcone-hosted-daemon-isolation.md](./lightcone-hosted-daemon-isolation.md) | 托管 Runtime 资源隔离：K8S + sandboxed runtime、风险分级、实现暂缓 |

---

## 核心命题

lightcone 的核心不是“多 Agent 聊天”，也不是“自动派发任务”，而是 **Context Governance**。

模型是商品，工具可以复制，普通多 agent 编排也会趋同。真正难复制的是一个团队长期积累并持续治理的 context：

- 目标和约束；
- 团队决策；
- 用户偏好；
- 项目知识；
- 技能规范；
- 历史纠偏；
- 执行结果；
- 失败复盘；
- 跨 agent 协作状态。

因此，lightcone 的系统目标是：

> 把所有影响 agent 行为的信息，纳入可分层、可授权、可审计、可压缩、可评估、可纠偏的 Context Governance 系统。

---

## 核心结论

当前设计已经收敛以下原则：

- **Context Item** 是唯一可注入 context 抽象，但不取代 domain source of truth。
- **Context Lifecycle** 定义在 Context Item Projection 上，采用事件驱动治理。
- **Context Bundle** 是每次 agent 调用的不可变认知快照。
- **Context Eval** 是 Context Governance 的反馈回路。
- **Goal State** 是最高优先级 context，由 Human 设定，Orchestrator 维护治理记录。
- **Human** 是 Team Context 的最高主权者。
- **Team** 是默认 Context Boundary。
- **Orchestrator** 是 Context Governance Agent，任务调度是它的下游行为。
- **Worker** 是 context consumer 和 candidate context producer。
- **Frontend** 是 Context Control Surface，不只是聊天 UI。
- **Hosted Runtime** 长期采用 K8S + sandboxed runtime，当前工程实现暂缓。

---

## 角色与边界

### Human

Human 是 Team Context 的最高主权者。

Human 拥有：
- 确认权：确认 goal、constraints、长期偏好、重要纠偏、不可逆决策。
- 修正权：修正任何 active context。
- 废弃权：archive / supersede 旧 memory、旧 decision、错误 summary。
- 提升权：把普通对话提升为 constraint、preference、team norm、decision、knowledge。
- 查看权：查看当前 Team 的 active context。
- 解释权：追问 agent 使用了哪些 context。

Human-only context 包括：
- goal；
- constraints；
- irreversible decision；
- sensitive preference；
- security / privacy policy。

### Orchestrator

Orchestrator 是 Context Governance Agent。

它的第一职责不是派 Worker，而是维护 Team Context 的质量、生命周期、冲突裁决和长期一致性。

职责：
- 判断用户输入对 context 的影响；
- 创建 context proposal；
- 审核 candidate context；
- 管理 decisions、corrections、blockers、current_phase；
- 处理 context conflict；
- 维护 context health；
- 在需要执行任务时 dispatch Worker；
- 根据 Worker result 决定是否产生新的 candidate context。

Orchestrator 不能：
- 修改 Human-owned goal / constraints；
- 把自己的判断伪装成人类约束；
- 绕过 Context Lifecycle；
- 让 Worker / tool output 直接进入 active context；
- 删除审计历史。

### Worker

Worker 是执行者，不是 context 治理者。

Worker：
- 消费 Context Bundle；
- 执行单一任务；
- 产生 task result；
- 上报 blocker；
- 产生 candidate context。

Worker 不能：
- 直接修改 Goal State；
- 直接晋升长期 context；
- 自行读取其他 Team context；
- 自行裁决未解决冲突。

### System

System 是 lifecycle、权限、审计和隔离规则的执行层。

System 负责：
- 强制 Context Bundle 生成；
- 强制 Context Lifecycle 状态转换；
- 强制权限边界；
- 强制 runtime isolation；
- 记录 audit / eval / usage events。

---

## Team = Context Boundary

Team 是 lightcone 的默认 Context Boundary。

```
Team = Context Boundary + Collaboration Space + Governance Scope
```

Team 内共享：
- Goal；
- Constraints；
- Decisions；
- Corrections；
- Knowledge；
- Memory；
- Skill Norms；
- Blockers；
- Working Context。

跨 Team 默认隔离：
- Goal / constraints 默认不跨 Team；
- Memory 默认不跨 Team；
- Working Context 默认 Task-local / Team-local；
- Knowledge 可提升为 Organization Scope；
- Skill Norm 可提升为 Organization / User Scope；
- User Preference 可跨 Team，但不能覆盖 Team / Organization constraints。

Context scope：

```
Organization Scope
User Scope
Team Scope
Task Scope
```

Context Assembly 必须同时考虑：
- scope；
- authority；
- lifecycle；
- relevance；
- source risk。

---

## Context 架构

### 总体链路

```
Domain Source of Truth
  → Context Item Projection
  → Context Assembly
  → Context Bundle
  → Agent Prompt
  → Agent Result
  → Context Usage / Eval
  → Governance Feedback
```

### Domain Source of Truth

复杂业务对象保留自己的 domain model，例如：
- Goal State；
- Decisions；
- Corrections；
- Blockers；
- Skills；
- Knowledge documents；
- Session summaries；
- Task dispatch records。

这些对象不是都塞进 `context_items`，但凡是可能进入 agent context window 的信息，都必须投影为 Context Item。

### Context Item

Context Item 是唯一可注入 context 抽象。

它不是万能业务表，而是可注入 context 的统一治理信封。

必须覆盖八类信息：
- Identity：id、item_version、content_hash。
- Scope：scope_type、scope_id、team_id、task_id。
- Source：source_type、source_id、source_version、source_uri、projection_mode。
- Classification：type、layer、section、tags。
- Content：title、content、summary、rendered_text。
- Authority / Trust：authority、confidence、status、visibility、source_risk。
- Lifecycle：promoted_at、expires_at、superseded_by、archived_at、rejected_at。
- Retrieval / Usage：embedding_ref、keywords、priority、usage_count、last_used_at、last_eval_status。

最小必填：
- id；
- item_version；
- scope_type / scope_id；
- type；
- layer；
- section；
- status；
- authority；
- source_type；
- content 或 rendered_text；
- created_at / updated_at。

### Context Lifecycle

Context Lifecycle 定义在 Context Item Projection 上。

主状态：

```
candidate → active → superseded
          → archived
          → expired

candidate → rejected
```

事件：

```
context.detected
context.promoted
context.rejected
context.injected
context.used
context.conflicted
context.superseded
context.expired
context.archived
```

原则：
- Worker / tool output 默认只能创建 candidate。
- Human 可直接创建高权威 active context。
- Orchestrator 负责 candidate 的晋升、拒绝、替代、归档和冲突裁决。
- 注入不是 lifecycle 主状态，必须通过 Context Bundle 审计。
- Eval 反馈影响检索权重和 review 优先级，但不能绕过 authority 规则。

### Context 分层

```
Layer 1  FROZEN     永久不变      goal / constraints
Layer 2  STABLE     低频变化      skills / knowledge / norms
Layer 3  EVOLVING   任务级更新    decisions / corrections / phase
Layer 4  EPHEMERAL  session 级    conversation / tool outputs
```

预算不足时从下往上裁剪，高权威 context 不可静默跳过。

### Context 类型

```
Goal       我们要做什么
Knowledge  我们知道什么
Skill      我们怎么做
Memory     我们做过什么
State      我们现在在哪
Working    我们正在处理什么
```

实际 Context Item type 还包括：
- constraint；
- decision；
- correction；
- preference；
- norm；
- blocker。

---

## Context Assembly

Context Assembly 是 context 的策略引擎，不是简单的 `fetch + pack`。

职责：
- 按 scope 过滤；
- 按 authority 过滤；
- 按 lifecycle status 过滤；
- 按 source risk 过滤；
- 处理 context conflict；
- 检索和排序；
- 控制 token budget；
- 执行降级策略；
- 生成 Context Bundle；
- 在冲突、污染、缺失 critical context 时唤醒 Orchestrator。

### Hybrid Retrieval

Embedding 是派生索引，不是权威来源。

检索顺序：

```
scope filter
→ status filter
→ authority filter
→ type / layer filter
→ tag / keyword filter
→ semantic vector recall
→ rerank
→ Context Assembly 裁决
```

必须保留非 embedding fallback。

### Latency Profile

Context Assembly 按三层执行：

| 层级 | 内容 | 行为 |
|---|---|---|
| Required | goal、constraints、team boundary、task scope、critical corrections | 不可静默跳过 |
| Soft Required | relevant skills、current phase、recent decisions | 可降级，必须记录 |
| Opportunistic | long memory、extra knowledge、historical summaries | 可超时跳过，必须记录 |

调用场景 profile：
- interactive_orchestrator；
- worker_dispatch；
- context_review；
- background_maintenance。

所有 skipped / degraded 都必须进入 Context Bundle。

---

## Context Bundle

Context Bundle 是每次 agent 调用的不可变认知快照。

它记录：
- team；
- task；
- agent；
- goal_state；
- model；
- retrieval_profile；
- latency_profile；
- assembly_policy_version；
- token_budget；
- included context items；
- skipped context items；
- item_version；
- content_hash；
- rendered_text_snapshot；
- skip reason；
- conflict / risk / timeout 信息；
- rendered prompt snapshot。

Bundle 支持 audit replay，不要求 execution replay。

Rendered prompt snapshot 需要：
- 权限控制；
- redaction；
- retention policy。

前端通过 Bundle 支持：
- View Context Used；
- agent 行为解释；
- 调试和复盘。

---

## Context 冲突裁决

冲突裁决遵循：

```
authority + scope + lifecycle + recency + confidence + relevance
```

裁决优先级：

```
1. System / security policy
2. Organization policy / compliance
3. Human-confirmed constraints
4. Active Goal State
5. Active corrections
6. Active decisions
7. Stable skill norms
8. Confirmed knowledge
9. Memory
10. Working context
11. Candidate context
12. Raw tool output
```

Context Assembly 可自动处理低风险冲突。

Orchestrator 处理语义冲突和同权威冲突。

Human 必须处理：
- goal 变更；
- constraints 变更；
- security / privacy 冲突；
- 权限扩大；
- 不可逆操作；
- Human-owned context 变更。

Worker 默认只接收已裁决 context。

---

## Context 污染防御

采用 default-untrusted 策略。

默认只能进入 candidate 的来源：
- Worker output；
- tool output；
- web content；
- raw document；
- summary；
- raw model inference。

Context Item 记录 `source_risk`：

```
trusted
normal
untrusted
hostile
```

candidate 晋升必须经过：
- source risk check；
- authority check；
- conflict check；
- scope check；
- Human / Orchestrator review。

Prompt injection 不得进入 rendered context。

Summary 必须继承 source range 中最高 source risk。

---

## Context 压缩

压缩是受治理约束的 memory transformation，不是普通摘要。

以下内容不进入普通压缩流程：
- goal；
- constraints；
- security policy；
- active corrections；
- active blockers；
- human-confirmed decisions；
- unresolved conflicts；
- secrets / credentials。

压缩输出必须结构化：
- confirmed_facts；
- user_decisions；
- constraints_mentions；
- assumptions；
- open_questions；
- open_conflicts；
- rejected_options；
- risks；
- next_actions；
- source_range；
- compression_warnings。

压缩结果默认只是 summary artifact 或 memory candidate，不能自动晋升为 active knowledge / decision / constraint。

---

## Context Eval

Context Eval 是 Context Governance 的反馈回路，不是单纯指标看板。

基本样本：

```
Context Bundle
  + Agent Result
  + Context Usage Events
  + Orchestrator Review
  + Human Feedback
```

指标分组：
- Retrieval Quality；
- Assembly Quality；
- Context Usage；
- Failure Attribution；
- Compression Quality；
- Lifecycle Quality；
- Human Trust / Control。

Eval 结果反哺：
- retrieval weights；
- context priority；
- lifecycle review；
- compression policy；
- risk policy；
- assembly policy。

高风险 Eval 结论不能只依赖 LLM 自评。

---

## Goal State

Goal State 是最高优先级 FROZEN context。

数据概念：

```js
{
  goal: "...",
  constraints: [...],
  current_phase: "..."
}
```

治理记录：
- decisions；
- corrections；
- blockers。

这些治理记录概念上属于 Goal State，但存储上采用 append-only 表，由 Context Assembly 按相关性、最近性和状态提取注入。

写权限：
- `goal`：仅 Human；
- `constraints`：仅 Human；
- `current_phase`：Orchestrator；
- `decisions`：Orchestrator append-only；
- `corrections`：Orchestrator append-only；
- `blockers`：Worker 可上报，Orchestrator 关闭。

Goal State：
- 无条件注入；
- 永远在 window 前部；
- 不参与普通压缩。

---

## Agent 架构

### 实体模型

```
Team
  ├── Orchestrator
  │     ├── Goal State
  │     ├── Context Governance
  │     └── Task Dispatch Queue
  ├── Worker Agent[]
  └── Human[]
```

### Orchestrator

Orchestrator 是 context-first。

处理用户消息时：

```
User Message
  → 判断对 Context 的影响
  → 必要时创建 context proposal / correction / decision
  → 判断是否需要执行任务
  → dispatch Worker
  → 审核 Worker Result
  → 更新 Context / 回复用户
```

### Worker

Worker 只看到 Context Bundle。

Worker 输出：

```xml
<task-notification>
  <task-id>{task_id}</task-id>
  <status>completed|failed|blocked</status>
  <summary>简短摘要</summary>
  <result>Worker 的最终输出内容</result>
  <usage>
    <total_tokens>N</total_tokens>
    <duration_ms>N</duration_ms>
  </usage>
</task-notification>
```

Worker result 默认不会直接进入长期 context，而是生成 candidate context 或 blocker。

---

## 前端：Context Control Surface

lightcone 前端不是普通聊天 UI，而是 Context Control Surface。

核心区域：

```
Chat / Command Surface
Context Surface
Execution Surface
```

### Chat / Command Surface

用户通过自然语言：
- 提需求；
- 纠偏；
- 追问；
- 确认 goal；
- 回复 Orchestrator；
- 提出长期规则。

### Context Surface

展示当前 Team 的 active context：
- Goal；
- Constraints；
- Active Corrections；
- Decisions；
- Team Norms；
- Knowledge；
- Memory；
- Open Blockers。

### Execution Surface

展示：
- Orchestrator 判断；
- Worker task；
- blocked / completed 状态；
- candidate context；
- context changes。

### 关键交互

- Context Proposal：Human confirm / edit / reject / change scope / change type。
- Context Inspector：查看单条 context 的 source、authority、status、last used、superseded_by。
- View Context Used：从 Context Bundle 展示一次 agent 调用实际使用了哪些 context。

Mode A / Mode B 只是可见度差异：
- Mode A：Outcome + Key Context Changes。
- Mode B：Outcome + Context + Worker Task Trace。

---

## 托管 Runtime

托管 runtime 不是当前阶段的实现重点。

长期方向：

```
K8S
  + sandboxed RuntimeClass
  + Hosted Sandbox Manager
  + Runtime Image
  + Runtime Profile
  + Workspace Volume
  + Secret Broker
  + Egress Proxy
```

边界：

```
Context Boundary:
  Team

Runtime Isolation Boundary:
  tenant / user / hosted workspace
```

当前结论：
- K8S 只作为 hosted execution plane。
- lightcone core 保持 runtime-backend agnostic。
- nsjail 降级为 local/dev 或 lightweight backend。
- 工程实现暂缓。

详见 [lightcone-hosted-daemon-isolation.md](./lightcone-hosted-daemon-isolation.md)。

---

## 基础设施总览

```
Event Layer
  → Context Assembly Service
  → Context Store
  → Context Bundle
  → Agent Runtime
  → Context Usage / Eval
  → Governance Feedback
```

主要服务：
- Context Assembly Service；
- Context Store；
- Orchestrator Scheduler；
- Task Dispatch；
- Context Eval；
- Hosted Sandbox Manager（托管版后续实现）。

---

## 当前暂不处理

- 迁移路径；
- Hosted Runtime 工程实现；
- embedding provider 具体选型；
- latency profile 具体数值；
- 前端视觉细节；
- K8S runtime 具体 RuntimeClass 选型。

这些问题不影响当前 Context Governance 架构收敛。
