# 真实频道回放分析：dev 与 atoll-research

日期：2026-09-07。此次是只读分析，不修改查询实现，不重启线上服务。

后续实施记录（同日）：正文/raw 双视图、状态分类与关系追读已实现，见 LOG_QUERY.md。
同一历史快照回放验证：research 的指定 RSI 回答在 conversation 搜索模型名时不再误命中，
raw 仍可命中；dev 原编辑消息的旧请求标为 replaced，可沿关系找到 seq 24550 的新正文。
以下统计与问题描述保留为改造前的基线，不代表新版投影计数。

## 方法与范围

线上 system_describe 尚未公布 system.log.query。用 sqlite3 -readonly 的在线 backup
取得一致性临时副本，在副本上执行当前 queryLog / ReadVisibleExchanges / ReadVisibleWindow
真实实现。每次调用遵守 5 秒预算；不是把手写 SQL 的结果冒充新工具的输出。
另外用只读 SQL 核对原始行数、消息类型和结束状态。临时诊断代码及副本已清理。

完整统计分别固定以下快照，逐页累加，直到 has_more=false：

| | dev | atoll-research |
|---|---:|---:|
| 快照 head_seq | 27393 | 15755 |
| 原始账本行数 | 27393 | 15755 |
| 原始记录起点（UTC） | 08-24 11:16:29 | 08-24 11:46:14 |
| 原始记录终点（UTC） | 09-07 03:18:40 | 09-07 02:41:18 |
| 当前投影消息数 | 2383 | 3107 |
| 当前投影问答/独立事件单元数 | 1335 | 1588 |
| 扫描候选单元数（含被排除的控制消息） | 4321 | 2847 |
| 完整统计所需页数 | 9 | 6 |
| sender 全页统计耗时 | 1942 ms | 1397 ms |
| sender 全部响应 JSON 累计大小 | 8387 bytes | 3932 bytes |

数字是当前投影口径，不是自然语言发言数，更不是独立话题数：它仍含无正文结束包、
终端事件、工具消息等。原始行数与投影数的差额不都等于“无用消息”，还包括被压缩的过程历史。
耗时是在本机副本上的暖缓存观测，不含模型、多轮工具调度、网络与线上并发成本，不是性能保证。

## 多角色上下文缺口确实存在

按完整历史 Actor ID 累加，主要发送者的投影条目为：

| 发送者 | dev | atoll-research |
|---|---:|---:|
| human:root | 1225 | 1585 |
| Claude | 634 | 1283 |
| Codex | 408 | 234 |
| 其他（工具、peer、system） | 116 | 5 |

research 的 1517 条 Agent 投影记录中，1283 条来自 Claude，约 84.6%。这不能直接推算
Codex 实际缺少多少 context（有转述、人工复制、历史注入），但说明只查自己的轮次覆盖面很窄。

dev 除 agent.ask 的 1812 条投影记录外，还包含 terminal.command 208 条、terminal.session
79 条、kimi.command 90 条、device.exec 70 条、agent.replace 46 条等。
research 主要是 agent.ask 2958 条，另有 agent.replace 74 条等。
两种工作流不同：研究频道以长讨论为主，开发频道中对话与操作事件混杂。

## 实际查询案例

### dev：找到自己未参与的 Actor Channel 推导

输入：`{"text":"actor channel","sender_kind":"human","head_seq":27393,"limit":3}`。
扫描 212 个候选，109 ms，找到：

- seq 26850：用户向 Codex 要此前设计总结；配对回答 seq 26858。
- seq 25628：用户向 Claude 强调“当然希望他是成为成员”；配对回答 seq 25632。
- seq 25587：用户向 Claude 提出 peer/svc 与 actor channel 的区别；配对回答 seq 25593。

这验证了查询范围不是调用 Agent 自己的问答。用 around_seq=25628、radius=5，7 ms
取回前后讨论，可以继续看到用户要求重新梳理，以及“连 atoll 本身都能够 as actor”的补充。
孤立关键词命中包含早期被推翻的方案，因此需要继续读修正过程，不能把命中即当成最新结论。

### research：相邻记录不是同一话题

输入：`{"around_seq":15323,"radius":3,"head_seq":15755}`。
扫描 46 个候选，观测 6–11 ms。定位点是 Codex 关于 RSI 与 Dalek 的回答。
后面三条却是：

- seq 15336：终端会话关闭。
- seq 15343：另一请求的 unanswered_timeout 结束包。
- seq 15366：用户开始讨论 DSH 插件结构。

不能把这三条解释成对 RSI 回答的后续讨论。时间邻近适合补现场，但不替代回复关系或话题线程。
当前结构保留 parent_id 是必要的；下一步还需要方便沿消息 ID 追查关系。

### research：元数据造成正文检索误命中

输入：`{"text":"gpt-5.6-sol","head_seq":15755,"limit":1}`。
扫描 125 个候选，67 ms，命中 seq 15323。实际讨论内容是 RSI，命中位于 usage.model。
当前规则确实是全文 JSON 子串，并非实现违反接口；但这个默认值不适合补对话 context。

### dev：编辑记录必须保留，但要表达替换关系

输入：`{"text":"背景半透明","head_seq":27393,"limit":2}`。
找到 agent.replace 请求 seq 24550 与对应回答 seq 24564；扫描 512 个候选后返回
has_more=true、scan_limited=true、next_before_seq=22792，耗时 179 ms。

编辑请求包含 target、old_text、new_text：旧内容询问图标用途，新内容要求半透明背景等。
整体把 agent.replace 归为 housekeeping 会丢用户真实指令；整体当普通正文搜索又会把旧稿
与新稿混在一起。需要显示“替换了哪条消息”，而不是简单保留/删除整个 type。

## 需要优先补的语义

1. **正文检索与原始 payload 检索分开。** 默认查询用户 body.text、Agent text 等会话正文；
   usage、浏览器 origin、控制参数不参与普通文字搜索。保留显式 raw 模式用于诊断。
   未知工具类型不能直接全丢，按其通用 JSON 或已知字段退回，并标明来源。
2. **给问答结果标明状态。** 快照中 dev 的 902 个 agent.ask terminal 有 222 个没有顶层 text，
   research 的 1479 个 terminal 同样有 222 个没有顶层 text。分别含 preempted_by 109/114 个，
   其余还见 replaced_by、merged_into、unanswered_timeout 等。不能把 terminal=true 等同于
   “这里有回答”，也不能为了有正文就复活旧的过程包。保留用户问题并标明中断/合并/失败状态。
3. **消息关系可追读。** parent_id、body.target、merged_into、replaced_by、preempted_by 都用
   消息 ID 表达关系；当前 read_seq 要求 seq，Agent 拿着 ID 不好继续。适合在同一查询词中
   增加 ID 定位及有限深度关系追踪，明确区分“回复”“编辑”“合并”“取代”，不猜造线程。
4. **区分会话与操作记录。** 默认补语义上下文时不让 terminal.session 等事件挤占邻居窗口；
   排查执行问题时仍能显式查看。应跨所有角色，而不是只保留 human/agent 两类发送者。

## 性能与实现取舍

sender/message_type/day 三次完整统计，dev 每次约 1.8–1.95 秒、research 约 1.3–1.4 秒；
观测的最慢统计单页约 346 ms。这个量级下扫描算法本身暂未构成首要阻碍。
但全局概览需要 6–9 次 Agent 往返，比一次本地扫描更贵：后续可以在已知快照下增加
受预算约束的汇总，或在 UI/Agent 侧自动累计，不应让“统计一部分”伪装成精确全量。

暂不建议仅凭这两份样本马上引入向量检索、独立搜索服务或任意 SQL。
先把会话正文、结束状态和关系追踪做准确，再评估数据规模或检索召回是否需要新索引。

此次只提交分析产物到工作区；上述改进尚未实施，也未进行 git commit。
