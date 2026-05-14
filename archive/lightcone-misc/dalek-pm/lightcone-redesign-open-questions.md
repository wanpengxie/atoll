# lightcone 重构：待讨论问题清单

> 所属设计文档索引：[lightcone-redesign.md](./lightcone-redesign.md)
> 状态：逐项讨论中
> 最后更新：2026-04-19

---

## 一、Orchestrator 本身的问题

### Q1：Orchestrator 自身的 context 膨胀 ✅

**结论：不需要单独设计。Orchestrator 是 Context Assembly Service 的一个消费者，和 Worker 走同一套组装流程。**

Orchestrator 每次被唤醒时：
```
assembleContext(orchestratorId, teamId, taskDesc, budget)
  → Goal Context（FROZEN 层，无条件注入，不压缩）
  → State Context（decisions + corrections，从 DB 读取）
  → Working Context（最近对话 + task-notifications，超限压缩为摘要）
```

Goal State 和 decisions 不会丢——它们存在 DB 里，属于 FROZEN/EVOLVING 层，每次重新注入。Working Context 超限就压缩，和 Worker 完全一样。

对比 Claude CLI：它没有 Context Assembly Service，靠 autocompact 暴力压缩整个对话，Goal 信息可能被压掉。lightcone 的分层设计天然解决了这个问题。

---

### Q2：Orchestrator 故障恢复 ✅

**结论：不需要 checkpoint，靠 DB 状态 + Context Assembly Service 重建。**

Orchestrator 重启后的恢复流程：
```
1. Goal State → 从 team_goal_state 表读取（FROZEN，不丢）
2. decisions / corrections → 同上（EVOLVING，DB 持久化）
3. 活跃任务 → 从 task_dispatch 表读取 status=in_progress 的记录
   → 注入到 State Context，Orchestrator 知道"我有哪些 Worker 在跑"
4. Working Context → 从最近消息历史 + session_summaries 重建
5. Context Assembly Service 组装完整 context → Orchestrator 恢复工作
```

Worker 在 Orchestrator 不在线时：
- Worker 继续执行（它不依赖 Orchestrator 在线）
- task-notification 写入 task_dispatch 表（持久化，不丢）
- Orchestrator 恢复后，Context Assembly 把未处理的 notification 注入

与 Claude CLI 的区别：Claude CLI 的 Orchestrator 是用户终端 session，用户关了就没了，靠 `--resume` 手动恢复。lightcone 的 Orchestrator 是后台长驻服务，必须自动恢复，用户不应感知中断。

---

### Q3：Orchestrator 的 system prompt 设计 ✅

**结论：全平台统一，七条行为规则，详见 [orchestrator-system-prompt.md](./orchestrator-system-prompt.md)。**

- 全平台统一，不按 Team 定制
- Goal State 通过 Context Assembly 注入，不在 prompt 中重复
- 七条规则覆盖：是否派 Worker、任务分解、并发控制、处理结果、纠偏判断、上报人类、Goal State 更新
- 规则写成 if-then 格式，减少 LLM 自由发挥导致的行为漂移

---

### Q4：Orchestrator 的成本控制 ✅

**结论：**
- **路由**：所有消息都经过 Orchestrator，不加路由层（Claude CLI 做法，架构简单）
- **付费**：用户承担 Orchestrator 的 token 消耗
- **模型**：Orchestrator 使用的模型可配置，后续根据实际效果调整（可以用小模型降成本）

---

## 二、Worker Agent 运行时细节

### Q5：Worker Agent 的运行时定义 ✅

**结论：采用 Claude CLI 做法——一次性短命 agent。**

```
dispatch_worker → Worker 执行任务 → 返回 task-notification → 销毁
需要续接时 send_to_worker 复用上下文（不重启）
没有超时，Worker 跑到完成或失败为止
Worker 有完整 bash 能力
```

---

### Q6：Worker 错误处理和重试 ✅

**结论：采用 Claude CLI 做法——不自动重试，Orchestrator 决策。**

```
Worker failed → Orchestrator 分析原因
  → 方法问题 → send_to_worker 纠正（Worker 保留错误上下文）
  → 定义问题 → stop + 重新 dispatch
  → 连续失败 2 次 → 上报用户
```

已在 Orchestrator system prompt 规则四中覆盖。

---

### Q7：多 Worker 并发冲突 ✅

**结论：采用 Claude CLI 做法——纯靠 Orchestrator 管理，不加系统层锁。**

Orchestrator system prompt 规则三已覆盖：
```
读并行、写串行、不确定就串行
Orchestrator 自己负责不派发冲突的写任务
系统不做自动冲突检测
```

---

## 三、Goal State 生命周期

### Q8：Goal State 的初始化 ✅

**结论：Orchestrator 从用户消息中提取 goal，提取后让用户确认。**

```
用户发第一条消息："帮我搭建一个电商后台"
→ Orchestrator 提取 goal + constraints
→ 回复用户："我理解你的目标是 X，约束是 Y，确认吗？"
→ 用户确认 → 写入 Goal State → 开始派发 Worker
→ 用户修正 → 更新后再确认
```

Goal State 为空时，Orchestrator 不派发 Worker，先完成 goal 确认。

---

### Q9：Goal 变更和版本管理 ✅

**结论：创建新版本的 Goal State，旧版本归档，变更前需用户确认。**

```
用户："之前说做电商后台，现在改成做社区论坛"
→ Orchestrator 识别到 goal 变更意图
→ 回复用户确认："你要把目标从 X 改为 Y，确认吗？旧目标下的进展将归档。"
→ 用户确认 → 旧 Goal State 归档（version=1, status=archived）
              新 Goal State 创建（version=2, status=active）
              decisions/corrections 从零开始
→ 触发所有活跃 Worker 停止（旧目标下的任务不再有效）
```

需要在 team_goal_state 表增加 version 和 status 字段。

---

## 四、基础设施缺失

### Q10：向量 embedding 的方案选型 ◐

**当前定位：原则已定，具体 provider 待选型。**

经过 Context Governance 设计补充后，embedding 不再是 context 管理的第一性问题。它是 Knowledge / Memory / Skill 语义检索的一种实现手段，必须服从 Context Item、Context Lifecycle、Context Bundle 和 Context Eval 的整体设计。

结论：
- Embedding 是 Context Retrieval 的派生索引和语义召回手段，不是 context 的权威来源。
- Context Item 的 `content`、`type`、`layer`、`authority`、`status` 才是权威数据。
- Context 检索必须采用 hybrid retrieval：先按 scope / status / authority / type / layer / tag 过滤，再做语义召回和 rerank。
- 最终是否注入由 Context Assembly 裁决，不能由向量相似度单独决定。
- Embedding provider 必须可插拔，并保留非 embedding fallback。

Context 检索依赖向量 embedding（Knowledge、Memory、Skill 的语义检索）。

后续技术选型：
- 用 OpenAI text-embedding-3-small？引入外部 API 依赖
- 用本地 embedding 模型（如 bge-small）？需要部署推理服务
- 国内用户的可用性？
- embedding 维度和存储方案（MySQL JSON 列性能够吗）？
- 是否有不依赖向量的 fallback？

---

### Q11：Context Assembly 的性能 ◐

**当前定位：原则已定，待定义 latency profile 和降级策略。**

Context Assembly 是 agent 调用前的关键路径，但性能优化必须服从 Context Governance。系统应按 Required / Soft Required / Opportunistic 分层执行。高权威 context 不可静默跳过；低权威或可选 context 可以缓存、降级或超时跳过，但必须写入 Context Bundle。缓存必须基于 context version、team boundary、agent role 和 retrieval profile 失效。不同调用场景应使用不同 latency profile。

建议分层：

| 层级 | 内容 | 行为 |
|---|---|---|
| Required | goal、constraints、team boundary、task scope、active critical corrections | 不可静默跳过 |
| Soft Required | relevant skills、current phase、recent decisions | 可降级，但必须记录 |
| Opportunistic | long memory、extra knowledge、historical summaries | 可超时跳过，但必须记录 |

建议 latency profile：
- `interactive_orchestrator`：用户正在等待回复，低延迟优先，但 required context 不可丢。
- `worker_dispatch`：Worker 开始执行前，可以花更多时间拿完整 context。
- `context_review`：Orchestrator 审核 candidate / conflict，可更慢更完整。
- `background_maintenance`：摘要、embedding、eval、archive，可异步。

每次调用前做 6 层并行检索。

待讨论：
- 6 个并行查询的总延迟预期？
- 向量检索在 MySQL 应用层做余弦相似度的性能？
- 是否需要缓存（Goal/State 变化少，可以缓存）？
- 对用户体感延迟的影响？

---

### Q12：迁移路径 ⏸

**当前定位：暂不讨论。**

当前处于方案设计阶段，先不考虑迁移、兼容和 MVP 分期。该问题保留，但不参与当前架构收敛讨论。

从当前架构到新架构的迁移策略。

待讨论：
- 全量重写还是增量迁移？
- 现有 agents、teams、messages 数据怎么处理？
- 新旧架构能否并行运行（灰度切换）？
- 迁移过程中的数据一致性？

---

## 五、用户体验层

### Q13：前端交互设计 ✅

**当前定位：基于 Q26 待细化。**

Q26 已确定：Human 是 Team Context 的最高主权者。前端问题不只是 Mode A / Mode B 的界面形态，而是用户如何理解、确认、查看、修正、废弃、提升和追问系统中的长期 context。

**结论：lightcone 前端应从普通 chat UI 升级为 Context Control Surface。**

聊天是主要输入入口，但用户必须能查看 active Team Context、确认 context proposal、修正 / 废弃 / 提升 context，并通过 Context Bundle 追问 agent 行为依据。

核心区域：
- Chat / Command Surface：用户用自然语言提需求、纠偏、追问、确认 goal、回复 Orchestrator。
- Context Surface：展示当前 Team 的 Goal、Constraints、Active Corrections、Decisions、Team Norms、Knowledge、Memory、Open Blockers。
- Execution Surface：展示 Orchestrator 判断、Worker task、blocked / completed 状态、candidate context。

Mode A / Mode B 不是两套执行模型，而是 context 与 execution trace 的可见度差异：
- Mode A：Outcome + Key Context Changes。
- Mode B：Outcome + Context + Worker Task Trace。

关键交互：
- Context Proposal：Orchestrator 建议把某条信息保存为 constraint / preference / team norm / decision / memory / knowledge，Human 可 confirm / edit / reject / change scope / change type。
- Context Inspector：查看单条 context 的内容、类型、scope、authority、source、status、created by、last used、superseded by、related decisions / corrections。
- View Context Used：从 Context Bundle 展示某次 agent / Worker 调用实际使用了哪些 context，以及哪些 context 被 skipped / degraded。

设计原则：Context Surface 不能做成复杂后台；默认展示高权威、active、最近变化的 context，低价值 memory 和 archived context 默认折叠。

新架构下前端需要配合的变化。

待讨论：
- Mode A（Orchestrator 视图）在 UI 上长什么样？
- Mode B（透明视图）在 UI 上长什么样？
- Goal State 有编辑界面吗？
- Worker 在执行时，用户看到什么？loading？进度条？实时 log？
- Orchestrator 在"思考"时，用户看到什么？

---

### Q14：多 Team 和跨 Team 场景 ✅

**当前定位：基于 Q27 待细化。**

Q27 已确定：Team 是 lightcone 的默认 Context Boundary。多 Team 和跨 Team 场景后续只需要在该原则下细化 User / Organization / Team / Task scopes 的共享、隔离和提升规则。

**结论：多 Team 架构采用 scope-based context governance。**

Team Scope 是默认 context boundary；Goal、constraints、memory、working context 默认 Team-local，不跨 Team。User Scope 保存个人偏好，Organization Scope 保存组织级知识、规范和政策，Task Scope 保存临时执行上下文。

建议 scope：

```
Organization Scope
User Scope
Team Scope
Task Scope
```

默认共享规则：
- Goal / constraints：默认 Team-local，不跨 Team。
- Memory：默认 Team-local，不跨 Team。
- Working context：默认 Task-local / Team-local，不跨 Team。
- Knowledge：可提升为 Organization Scope，但需要确认。
- Skill norm：可提升为 Organization Scope 或 User Scope，但需要确认。
- User preference：可跨 Team，但不能覆盖 Team / Organization constraints。
- Security / compliance policy：可从 Organization Scope 注入所有 Team。

跨 Team 共享必须通过 explicit context proposal、Human / policy 确认和 Context Item projection，不允许直接读取其他 Team 的 memory。

Worker 不携带跨 Team 长期记忆，只消费当前 Team / Task 的 Context Bundle。Context Assembly 必须同时考虑 scope、authority、lifecycle 和 relevance。

一个用户有多个 Team，每个 Team 各自的 Orchestrator。

待讨论：
- 跨 Team 的知识能共享吗？
- 用户级别的记忆 vs Team 级别的记忆？
- 一个 Worker Agent 能同时属于多个 Team 吗？
- Team 之间的 Orchestrator 能协作吗（嵌套 Orchestrator）？

---

## 六、隔离方案补充

### Q15：K8S sandboxed runtime 集成方式 ✅

lightcone server 是 Node.js 进程，需要托管用户的 agent runtime。

**结论：主托管方案直接采用 K8S + sandboxed runtime，由 Hosted Sandbox Manager 通过 K8S API 管理 runtime。方案方向已定，工程实现暂缓。**

lightcone server 作为 control plane，只请求创建、启动、停止和观测 hosted runtime；Hosted Sandbox Manager 作为 execution runtime control，负责通过 K8S API 创建 namespace / pod / PVC / NetworkPolicy / ResourceQuota，并选择 sandboxed RuntimeClass。

抽象：

```
lightcone server
  = control plane

Hosted Sandbox Manager
  = execution runtime control

K8S
  = orchestration layer

gVisor / Kata / Firecracker-based runtime
  = isolation layer
```

产品抽象仍然是 `HostedRuntimeSandbox`，不是 K8S Pod 本身：

```
HostedRuntimeSandbox
  - create
  - start
  - stop
  - restart
  - health
  - logs
  - resource usage
  - destroy
```

权限原则：
- lightcone server 默认无特权运行。
- Sandbox Manager 是受控的 runtime control 组件，只通过 K8S API 管理受限资源。
- Runtime profiles 由配置定义，映射到 image、RuntimeClass、PodSpec、NetworkPolicy、ResourceQuota、PVC 和 Secret policy。
- 所有启动、停止、失败、资源超限都要有审计事件。
- nsjail 降级为 local/dev 或 lightweight backend，不作为主要 hosted production 方案。

边界区分：

```
Context Boundary:
  Team

Runtime Isolation Boundary:
  tenant / user / hosted workspace
```

Team 决定 agent 看到哪些 context；tenant / user / hosted workspace 决定 agent 能访问哪些运行时资源。两者不能混淆。

K8S 映射：
- Runtime Image → container image。
- Runtime Profile → RuntimeClass + PodSpec + ResourceQuota + NetworkPolicy。
- User Workspace → PVC / ephemeral volume。
- Secret Broker → K8S Secret / External Secret / sidecar broker。
- Egress Policy → NetworkPolicy + egress proxy。
- Browser Capability → same pod container or sidecar。

---

### Q16：workspace 基础环境 ✅

Hosted runtime 需要基础运行环境，支持用户通过 Claude / Codex / Kimi 等 CLI 操作目录、调用模型、使用浏览器和发送请求。

**结论：workspace 基础环境采用 K8S-native 的 Runtime Image + Runtime Profile + Workspace Volume，不基于 nsjail 自研容器平台。方案方向已定，工程实现暂缓。**

抽象：

```
HostedRuntimeSandbox
  = Runtime Image
  + Runtime Profile
  + Workspace Volume
  + Secret Broker
  + Egress Policy
```

映射：
- Runtime Image：OCI container image，包含基础 shell、git、node、python、CLI tools、browser dependencies、agent runner。
- Runtime Profile：K8S RuntimeClass、resource limits、network policy、browser capability、dependency policy。
- Workspace Volume：per user / tenant workspace 的 PVC 或 ephemeral volume。
- Secret Broker：按 tool / task / user scope 提供短期凭证，不把全量 secret 长期放进环境变量。
- Egress Policy：默认阻断内网、metadata service、其他 tenant workspace，只允许授权公网目标和模型供应商 API。

原则：
- 不自研 Docker / container platform。
- 使用 K8S 和容器生态管理 image、volume、resource、logs、health 和 lifecycle。
- 默认 runtime image 最小可用，不做全能镜像。
- CLI tools 和 browser capability 通过版本化 runtime image / profile 管理。
- 默认 workspace 可 ephemeral；需要长期项目状态时使用 persistent PVC，并配额治理。
- runtime isolation boundary 是 tenant / user / hosted workspace；context boundary 仍然是 Team。

后续细化：
- Runtime image 的基础工具集和版本策略。
- 用户依赖安装进入 dependency layer 还是 workspace layer。
- 不同 runtime profile 的能力边界，例如 browser-enabled、network-restricted、code-agent-standard。
- 基础镜像更新、回滚和安全补丁策略。

---

## 七、Context Governance 补充问题

### Q17：Context Item 的统一数据模型 ✅

**结论：Context Item 是可注入 context 的统一治理信封，而不是万能业务表。**

Q23 已确定：Context Item 是唯一的可注入 context 抽象，但不强行取代各类 domain source of truth。因此 Context Item 只保存可注入投影和治理元数据，复杂业务语义保留在 domain source of truth。

Context Item 必须包含八类基础信息：
- Identity：id、item_version、content_hash、created_at、updated_at。
- Scope：scope_type、scope_id、team_id、task_id。
- Source：source_type、source_id、source_version、source_uri、projection_mode。
- Classification：type、layer、section、tags。
- Content：title、content、summary、rendered_text、language。
- Authority / Trust：authority、confidence、status、visibility。
- Lifecycle：promoted_at、promoted_by、expires_at、superseded_by、archived_at、rejected_at。
- Retrieval / Usage：embedding_ref、keywords、priority、usage_count、last_injected_at、last_used_at、last_eval_status。

最小必填字段：
- id
- item_version
- scope_type / scope_id
- type
- layer
- section
- status
- authority
- content 或 rendered_text
- source_type
- created_at / updated_at

Context Bundle 必须引用 context item 的版本、content hash 和 rendered snapshot，保证历史调用可审计和可回放。

### Q18：Context 冲突裁决规则 ✅

**结论：Context 冲突裁决遵循 authority + scope + lifecycle + recency + confidence + relevance 的组合规则。**

System / security / org policy 和 Human-confirmed constraints 优先于普通 memory、knowledge 和 working context。Context Assembly 可自动处理低风险且规则明确的冲突；Orchestrator 负责语义冲突和同权威冲突裁决；凡涉及 goal、constraints、security、privacy、权限扩大、不可逆操作或 Human-owned context 变更的冲突，必须 Human 确认。

不同来源的 context 会发生冲突，例如旧 decision 与新 correction 冲突、Worker 发现与 stable knowledge 冲突、用户新约束与历史记忆冲突。

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

自动裁决范围：
- active vs archived / superseded：active replacement 赢。
- constraint vs memory：constraint 赢。
- new explicit decision supersedes old decision：new decision 赢。
- candidate vs active：active 赢，candidate 不注入给 Worker。

Orchestrator 裁决范围：
- active decisions 语义冲突；
- correction 和 decision 冲突；
- knowledge 和 memory 冲突；
- Worker result 和现有 knowledge 冲突；
- skill norm 和 task instruction 冲突；
- 同一 authority 下没有明确 supersede 关系。

冲突裁决必须转化为 Context Lifecycle 事件，并在 Context Bundle 中记录 skipped_conflict / winner / reason。Worker 默认只接收已裁决的 context；未裁决冲突只进入 Orchestrator 的 conflict summary。

### Q19：Context 压缩损耗控制 ✅

**结论：Context 压缩是受治理约束的 memory transformation，不是普通摘要。**

高权威 context、active corrections、active blockers、human-confirmed decisions、unresolved conflicts 和 secrets 不进入普通压缩流程。压缩输出必须结构化，至少包含 confirmed_facts、user_decisions、constraints_mentions、assumptions、open_questions、open_conflicts、rejected_options、risks、next_actions、source_range 和 compression_warnings。

Working Context 摘要会改变系统记忆，不能当成普通文本摘要处理。

原则：
- 压缩结果默认只是 summary artifact 或 memory candidate，不能自动晋升为 active knowledge / decision / constraint。
- 所有 summary 必须保留 source range，并支持 compression loss 检测。
- 压缩器不能自行裁决冲突；未裁决冲突必须进入 `open_conflicts`。
- 原文中的 assumption 不能被 summary 写成 fact。
- rejected options 不能被重新写成 next action。
- secrets / credentials 不能进入 summary。
- summary-derived memory candidate 需要 Orchestrator review 才能晋升。

### Q20：Context 污染防御 ✅

**结论：Context 污染防御采用 default-untrusted 策略。**

除 Human-confirmed 和 system-governed context 外，Worker output、tool output、web content、raw document、summary、raw model inference 默认只能生成 candidate，不能直接进入 active context。Candidate 晋升必须经过 source risk、authority、conflict、scope 和 review 检查。

Worker 输出、工具输出、网页内容、用户粘贴内容都可能包含错误信息或 prompt injection，不能自动进入长期 context。

默认规则：
- Human explicit confirmation：可成为高权威 active context。
- System-governed event：可成为技术性 active context。
- Orchestrator-reviewed：可成为 decision / correction / knowledge / memory。
- Worker / Tool / Web / Raw Document / Summary：默认只能 candidate。
- Raw model inference：默认不能直接成为 active。

Context Item 需要记录 `source_risk`：

```
trusted
normal
untrusted
hostile
```

晋升检查：
- Source risk check
- Authority check
- Conflict check
- Scope check
- Human / Orchestrator review

Prompt injection 内容应被标记和隔离，不得进入 rendered context。summary 必须继承 source range 中最高风险；跨 Team context 共享必须重新生成 projection 并保留来源风险。

### Q21：Context Bundle 审计与回放 ✅

**结论：Context Bundle 是每次 agent 调用的不可变认知快照。**

所有 Orchestrator / Worker / agent 调用必须生成 Context Bundle，记录 team、task、agent、goal_state、model、retrieval_profile、latency_profile、assembly_policy_version、token_budget、included context items、skipped context items、selection_status、item_version、content_hash、rendered_text_snapshot、skip reason、conflict / risk / timeout 信息。

每次 agent 调用实际注入的 context 必须可审计，否则无法解释 agent 为什么做出某个判断。

原则：
- Bundle 一旦生成不可变。
- Bundle 必须与 task_dispatch、agent_result 和 context_usage_events 关联。
- Bundle 支持 audit replay，但不要求 execution replay。
- rendered prompt snapshot 应保存，但受权限、redaction 和 retention policy 控制。
- 用户前端可通过 View Context Used 查看关键 context；完整 prompt 视权限开放。

### Q22：Context Eval 指标体系 ✅

**结论：Context Eval 是 Context Governance 的反馈回路，而不是单纯指标看板。**

Context Eval 以 Context Bundle 为基本样本，结合 agent result、context_usage_events、Orchestrator review 和 Human feedback，评估 retrieval quality、assembly quality、context usage、failure attribution、compression quality、lifecycle quality 和 human trust/control。

如果 context 管理是核心竞争力，就必须能评价 context 注入是否有效。

Eval 结果必须能反哺：
- retrieval weights
- context priority
- lifecycle review
- compression policy
- risk policy
- assembly policy

指标分组：
- Retrieval Quality：retrieval_precision、missed_critical_context、scope_leakage、authority_violation。
- Assembly Quality：required_context_missing、optional_context_timeout_rate、fallback_used_count。
- Context Usage：referenced_rate、ignored_rate、repeated_unused_context。
- Failure Attribution：context_induced_failure、missing_context_failure、stale_context_failure、contamination_failure、compression_loss_failure。
- Compression Quality：assumption_promoted_to_fact、conflict_collapsed、constraint_omitted。
- Lifecycle Quality：stale_active_context_count、superseded_but_injected_count、human_override_rate。
- Human Trust / Control：human_context_correction_rate、human_rejects_proposal_rate、human_asks_why_rate。

高风险 Eval 结论不能只依赖 LLM 自评，应结合系统信号、Orchestrator review 和 Human feedback。

---

## 八、核心架构收敛问题

### Q23：Context Item 是否是唯一底层抽象 ✅

当前设计里同时存在 Goal State、decision/correction/blocker、team_knowledge、agent_memory、skills、session_summaries 和统一的 context_items。它们都属于 context，但抽象层级还没有完全收敛。

**结论：Context Item 是唯一的可注入 context 抽象，但不是所有业务数据的唯一存储模型。**

采用双层模型：

```
Domain Source of Truth
  → Context Item Projection
  → Context Bundle
  → Agent Prompt
```

原则：
- 所有可能进入 agent context window 的信息，都必须先投影为 Context Item。
- Goal State、Decision、Correction、Blocker、Skill、Knowledge 等复杂对象可以保留自己的 domain source of truth。
- domain table 是写入和业务语义权威；context_items 是检索、注入、生命周期、权威标注和审计治理层。
- Context Assembly 只面向 Context Item 工作，避免分别理解各类业务表。
- Context Bundle 只引用 Context Item，保证每次 agent 调用的审计和回放统一。

这个结论避免两个极端：
- 不把所有东西塞进万能 `context_items` 表；
- 也不让各种 context 各管各的，导致 assembly、审计和 eval 碎片化。

### Q24：Context Lifecycle 的完整闭环 ✅

Context 系统不能只解决“如何注入”，还要定义一条完整生命周期：信息如何产生、确认、使用、评估、过期和归档。

**结论：Context Lifecycle 定义在 Context Item Projection 上。**

所有可注入 context 都经历统一生命周期，但 domain source of truth 可以保留自己的业务生命周期。Context Item 生命周期负责治理“这条信息是否可以进入 agent context window，以及如何被审计、替代、过期和归档”。

主状态：

```
candidate → active → superseded
          → archived
          → expired

candidate → rejected
```

状态含义：
- `candidate`：候选 context，可能有长期价值，但尚未确认。
- `active`：可被 Context Assembly 检索和注入。
- `rejected`：候选内容被拒绝，不进入长期 context。
- `superseded`：被新 context 明确替代，历史保留，默认不注入。
- `archived`：不再参与当前目标或当前 Team 工作流，历史保留。
- `expired`：到期失效，默认不注入，但可审计。

生命周期事件：

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

权限原则：
- Worker 和 tool output 默认只能创建 `candidate`，不能直接晋升为长期 active context。
- Human 可直接创建高权威 active context，例如 goal、constraint、preference。
- Orchestrator 负责 candidate 的晋升、拒绝、替代、归档和冲突裁决。
- System 可创建技术性治理记录，例如 bundle record、usage event，但不能自动创建高权威语义 context。

注入不是生命周期主状态。`active` 只表示“可注入”，实际注入必须通过 Context Bundle 记录为 `context.injected` 事件。

Eval 反馈影响 retrieval rank、future inclusion probability、confidence、review priority，但不能绕过权威规则直接改变高权威 context 的状态。

### Q25：Orchestrator 的第一职责是否应定义为 Context Governance ✅

当前 Orchestrator 设计偏任务调度：判断是否派 Worker、拆任务、处理结果、失败重试。若 lightcone 的核心是 context 管理，Orchestrator 的第一职责应进一步定义为维护团队共享 context 的质量。

**结论：Orchestrator 的第一职责是 Context Governance，而不是任务调度。**

Orchestrator 是 Team Context 的治理者，负责维护共享 context 的质量、生命周期、冲突裁决和长期一致性。任务分解与 Worker dispatch 是它在判断 context 和 goal 之后采取的执行手段。

职责：
- 判断用户输入对 context 的影响：goal、constraint、correction、preference、temporary instruction、decision。
- 审核 candidate context：promote / reject / archive / request human review。
- 管理 decision、correction、blocker 和 current phase。
- 处理 context conflict，必要时上报 Human。
- 判断 Worker result 是否应该影响长期 context。
- 维护 context health：发现过期、低价值、冲突、污染或 caused_failure 的 context。
- 在需要执行任务时构造 task 并 dispatch Worker。

权限边界：
- Orchestrator 不能修改 Human 拥有的 goal / constraints。
- Orchestrator 不能把自己的判断伪装成人类约束。
- Orchestrator 不能绕过 Context Lifecycle、Authority 和 Audit 规则。
- Orchestrator 不能让 Worker / tool output 直接进入 active context。
- Orchestrator 是治理执行者，不是最高权威；最高权威仍是 Human 和系统治理规则。

后续需要基于该结论重写 Orchestrator system prompt：从 task-first 改为 context-first。

### Q26：Human 对 Context 的所有权和干预方式 ✅

Human 不应只是 goal/constraints 的确认者，也应是长期 context 的最高权威来源和最终编辑者。否则系统会积累用户不可见、不可控的长期记忆。

**结论：Human 是 Team Context 的最高主权者。**

系统必须让 Human 能理解、确认、修正、废弃、提升和追问 context。Orchestrator 是 context steward / governance executor，负责提出、整理、晋升、裁决和维护 context，但不能取代 Human 对 goal、constraints 和高权威长期 context 的最终所有权。

Human 的核心权力：
- 确认权：确认 goal、constraints、long-term preference、team norm、important correction、irreversible decision。
- 修正权：修正任何 active context，例如“这个记忆不对”“这只是临时要求”。
- 废弃权：archive / supersede 旧 memory、旧 decision、过期 knowledge、错误 summary。
- 提升权：把普通对话提升为长期 context，例如 constraint、preference、team norm、skill rule、decision。
- 查看权：查看当前 Team 的 active context，包括 Goal、Constraints、Active Corrections、Decisions、Team Norms、Knowledge、Memory、Open Blockers。
- 解释权：追问 agent 或 Worker 使用了哪些 context，系统必须能通过 Context Bundle 回答。

角色边界：

```
Human       = context sovereign / final owner
Orchestrator = context steward / governance executor
Worker      = context consumer / candidate producer
System      = lifecycle and audit enforcer
```

不同 context 类型的人类干预级别：
- Human-only：goal、constraints、irreversible decision、sensitive preference、security / privacy policy。
- Human-overridable：decisions、corrections、team norms、active knowledge、memory、summary-derived context。
- System-managed：context bundle、usage event、eval event、injected / skipped records，可查看但通常不直接编辑。

用户说“以后都按 X 来”时，Orchestrator 应识别为长期规则候选，生成 context proposal，由 Human 确认类型和范围后再进入 active context。

所有 Human 干预都应转化为 Context Lifecycle 事件，并保留审计。

### Q27：Team 是否应定义为 Context Boundary ✅

当前 Team 是协作单元；从 context 架构看，Team 更应该被定义为共享 Goal、Memory、Skill Norm、Knowledge 和 Governance Policy 的边界。

**结论：Team 是 lightcone 的默认 Context Boundary。**

Team 不只是人类和 agent 的聊天空间，而是一组共享 Goal、Context、Memory、Knowledge、Skill Norm 和 Governance Policy 的边界。

定义：

```
Team = Context Boundary + Collaboration Space + Governance Scope
```

原则：
- 一个 Team 对应一个协作空间、一个治理范围、一个 active Goal State 和一个 Orchestrator。
- Team 内共享 Goal、Constraints、Decisions、Corrections、Knowledge、Memory、Skill Norms、Blockers 和 Working Context。
- 跨 Team 默认隔离；不能默认互通 memory、goal 或 constraints。
- User / Organization / Task 可以作为额外 scope，但必须通过显式规则参与 Context Assembly。
- Worker 看到的 context 由当前 `team_id` 和 `task_id` 决定，不能携带其他 Team 的 memory。
- 跨 Team 共享 context 必须通过 Human / Orchestrator proposal 和确认机制，不能直接读取对方 memory。

建议 scope 层级：

```
User Scope
Organization Scope
Team Scope
Task Scope
```

共享原则：
- Goal / constraints 默认不跨 Team。
- Memory 默认 Team-local。
- Knowledge 可提升为 Organization Scope，但需要确认。
- Skill Norm 可跨 Team，但不能覆盖 Team constraints。
- User Preference 可跨 Team，但不能覆盖 Team / Organization 约束。

---

## 讨论进度

| # | 问题 | 状态 |
|---|------|------|
| Q1 | Orchestrator context 膨胀 | ✅ 不需要单独设计，走 Context Assembly Service |
| Q2 | Orchestrator 故障恢复 | ✅ 靠 DB 状态 + Context Assembly 重建，不需要 checkpoint |
| Q3 | Orchestrator system prompt | ✅ 全平台统一，七条行为规则 |
| Q4 | Orchestrator 成本控制 | ✅ 不加路由层，用户付费，模型可配置 |
| Q5 | Worker 运行时定义 | ✅ 一次性短命 agent，有 bash，无超时 |
| Q6 | Worker 错误处理和重试 | ✅ 不自动重试，Orchestrator 决策，失败 2 次上报 |
| Q7 | 多 Worker 并发冲突 | ✅ Orchestrator 管理，不加系统层锁 |
| Q8 | Goal State 初始化 | ✅ Orchestrator 提取 + 用户确认 |
| Q9 | Goal 变更和版本管理 | ✅ 创建新版本 + 旧版本归档 + 用户确认 |
| Q10 | 向量 embedding 方案 | ◐ 原则已定，provider 待选型 |
| Q11 | Context Assembly 性能 | ◐ 原则已定，待定义 latency profile 和降级策略 |
| Q12 | 迁移路径 | ⏸ 暂不讨论 |
| Q13 | 前端交互设计 | ✅ Context Control Surface |
| Q14 | 多 Team 和跨 Team | ✅ scope-based context governance |
| Q15 | K8S sandboxed runtime 集成方式 | ✅ 方案已定，工程实现暂缓 |
| Q16 | workspace 基础环境 | ✅ 方案已定，工程实现暂缓 |
| Q17 | Context Item 统一数据模型 | ✅ Context Item 是可注入治理信封 |
| Q18 | Context 冲突裁决规则 | ✅ authority + scope + lifecycle 裁决 |
| Q19 | Context 压缩损耗控制 | ✅ 结构化压缩 + loss control |
| Q20 | Context 污染防御 | ✅ default-untrusted + source_risk |
| Q21 | Context Bundle 审计与回放 | ✅ immutable cognitive snapshot |
| Q22 | Context Eval 指标体系 | ✅ feedback loop based on Context Bundle |
| Q23 | Context Item 是否是唯一底层抽象 | ✅ Context Item 是唯一可注入抽象，不取代 domain source of truth |
| Q24 | Context Lifecycle 完整闭环 | ✅ 定义在 Context Item Projection 上，事件驱动治理 |
| Q25 | Orchestrator 是否应定义为 Context Governance Agent | ✅ Orchestrator 是 context-first 的治理者 |
| Q26 | Human 对 Context 的所有权和干预方式 | ✅ Human 是 context sovereign |
| Q27 | Team 是否应定义为 Context Boundary | ✅ Team 是默认 Context Boundary |
