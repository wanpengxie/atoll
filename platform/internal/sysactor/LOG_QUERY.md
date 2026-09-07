# Agent 的共享频道查询：system.log.query

2026-09-07：会话投影、状态、单跳关系追读及返回式用法提示已实现。加载新 server 与
承载 Agent 的 daemon 二进制后，通过 system_describe 发现。不是新增 HTTP API 或独立服务。

## 定位与调用链

Channel 是类似 Slack 的多角色共享空间；每个 Agent 的 context 往往只覆盖自己的轮次。
本工具查询整个频道的可见历史，不隐含 sender=调用 Agent。

```text
system_describe → 发现 system.log.query 的描述、schema、examples
system_call → 普通 request / system.log.query → system actor
            → 本频道只读账本 → 普通 terminal response
```

通过现有 system_call 的参数格式提交下面的裸 payload。query 与结果照常落账，
housekeeping 排除自身，防止重复搜索自身结果。没有定时拉取；system.log.recent 保持原义。

## 查询工作流

| 目的 | 发给 system.log.query 的 payload 示例 |
|---|---|
| 了解谁参与了讨论 | `{"group_by":"sender"}` |
| 查关键词在哪些日期出现 | `{"group_by":"day","text":"actor channel"}` |
| 找具体问答 | `{"text":"actor channel","limit":5}` |
| 追回复、编辑、合并关系 | `{"related_to":"命中消息的id","head_seq":15000}` |
| 补其他角色的附近发言 | `{"around_seq":11980,"radius":3,"head_seq":15000}` |
| 读完整正文 | `{"read_id":"命中消息的id","head_seq":15000}` |
| 诊断原始元数据、旧正文 | `{"read_seq":11980,"view":"raw","head_seq":15000}` |

seq、ID、head_seq 应使用真实返回值，不照抄示例坐标。

## 两种视图

所有模式都接受 view 与 head_seq。

- conversation（默认）：user/Agent 正文；不把 usage.model、浏览器 origin、过程命令当
  正文搜索。agent.replace 只搜索 new_text，不混入 old_text。纯 terminal/ui 操作事件
  不进入搜索/附近窗口。未知工具结构仍可检索，标为 structured_json，不按 actor kind 全删。
- raw：规范化完整 JSON（含元数据、old_text、操作事件）。保持原有可见性与 housekeeping
  限制，搜索/附近仍选择每个请求的 terminal 或最新 provisional，并非无限制原始账本导出。
  精确 read 可以指定一个历史 provisional，不必是当时最新响应。

结果中的 content_source 为 text / replacement_text / superseded / structured_json / raw_json。
payload_text 是该表示的片段，不一定是 JSON。原始正文不回写账本。

## 搜索、统计与状态

搜索条件：text（去两端空白，不区分大小写的字面子串，最多 256 字符）、sender（完整历史
Actor ID）、participant（sender 或明确 audience 命中，不推断广播读者）、sender_kind、
message_type、from_ts（服务端接收时间，Unix 毫秒，包含）、to_ts（同上，不包含）。
所有条件必须在同一行成立；matched_seqs 标明命中行，配对行只是上下文。

至少提供条件或 before_seq；limit 默认 5，最大 20。turns 按请求/独立事件 seq 降序，
每项请求带直接 terminal/latest 回复。嵌套请求独立成项，不假装是完整根线程树。

turn.state / message.state 的值：

| 值 | Agent 应如何理解 |
|---|---|
| processing | 未观察到可见结束；不代表已有答案 |
| answered | 结束回复有 text |
| interrupted | 被后续输入打断；沿 preempted_by 查看后续，但不推断旧任务已解决 |
| replaced | 原请求被替换；沿 replaced_by 查看新请求 |
| merged | 合并进另一请求；沿 merged_into 查看承接者 |
| failed | 失败/取消；错误事实不等于正文回答 |
| completed_without_answer | 已结束但没有正文，不猜测成功回答 |

已替换原请求仍保留身份、状态和关系，但 conversation 不再返回/搜索其旧正文，
content_source=superseded。判断依据是原请求的结束记录 replaced_by，不是一次可能失败的
编辑请求。编辑前 head_seq 仍看到旧内容；raw 可显式回看旧版本。
合并不等于编辑，merged 的原输入仍保留。空状态包附在问答中，但不占 conversation 附近的回复名额。

统计 group_by 支持 sender、sender_kind、message_type、day（UTC）。可附搜索条件，
不接受 limit。stats.scope 固定 scanned_page；各字段：

- matched_messages：匹配消息数，含状态包，不是正文发言数。
- matched_exchanges：含匹配消息的问答/事件单元数，不是话题数。
- questions：匹配的 request 行数，包含工具请求。
- answers：匹配的 response 中有顶层 text 的行数。
- states：匹配单元的请求状态，每单元一次；独立事件不计。
- buckets：key、messages、first_seq/last_seq、from_ts/through_ts；时间两端均包含。

保留相同 view/条件/group_by/head_seq，通过 next_before_seq 逐页累加，直到 has_more=false。
同 key 的 messages、questions/answers/states 可相加；不要混快照、重复累计或把局部计数叫全量。

## 附近与关联不同

around_seq：前后各 radius 条（默认 3，最大 10），跨所有角色按消息 seq 排序。上下文模式
不能带 text/sender 等过滤条件，防止把异议或补充筛掉。附近只是现场，不等于同一个话题。
空的终答包在 conversation 中通过问题状态表现，不占邻居名额；显式 anchor 仍可读取它。

context 返回 before、anchor、after，及两侧游标与 has_older/has_newer、scan_limited 标志。
续查保留原 around_seq/head_seq，并同时带回 next_before_seq/next_after_seq。
anchor 会重复，邻居不重复；一侧无结果但 scan_limited=true 时仍可继续。

related_to：以消息 ID 取 anchor + 一跳关系。识别 parent_id、请求的选定 response，以及
Agent 协议里的 edits、replaced_by、merged_into、preempted_by。不在任意工具 JSON/正文
中猜关系，不自动递归。对返回 ID 再查即可多轮追踪；调用方维护 visited IDs，避免循环。
同一跳按目标 ID 去重；若有多个边指向同一 ID，anchor.relations 保留这些边。
关系目标不存在、不可读或位于快照之后，统一 unavailable，不暴露原因。

## 返回结果教 Agent 如何继续

描述层：manifest 的 description + 参数说明 + examples + output_schema 描述上述流程。
共同 toolsurface 给 Claude/Codex 同样的用法提醒，不分 provider 实现。

每次成功返回还包含：

- guidance：解释视图、状态、统计范围、历史非指令、附近非线程等。
- next：`[{reason,request}]`，request 是可直接再次提交给本词的合法裸参数。
  搜索/统计续页保持所有条件；附近续页保持两侧坐标；还给首个结果的关系追读、附近和 raw 入口。
- 每条长消息的 next_read：完整续读参数，保留 view/head_seq 并推进 offset。
- 每条消息的 relations：明确边类型和 message_id，可自行选择不是第一个结果的关系继续查。

这些只是提示，不是已经执行的动作，也不是历史正文发出的指令。

示意：

```json
{
  "view":"conversation",
  "head_seq":15000,
  "has_more":true,
  "next":[{
    "reason":"Continue older candidates with identical filters.",
    "request":{"text":"actor channel","before_seq":12000,"head_seq":15000,"view":"conversation"}
  }]
}
```

完整消息仍包含 seq/id/sender/audience/kind/type/parent_id/receipt time，用于归因。
每段 search/context 最多 1500 Unicode 字符，read 最多 8000。用 next_read 连续拼接表示；
改变 view 后从 offset=0 重新读取，不能混合正文偏移与原始 JSON 偏移。

## 边界、预算与错误

每次搜索/统计扫描最多 512 候选单元或约 8 MiB；附近每方向同样预算（按候选消息计）。
整体 5 秒预算。scan_limited/has_more 表明还有候选，空页不能证明全历史没有内容。
所有读都受 head_seq 限制，默认捕获当前快照。无 channel_id，不新增跨频道授权。
system visibility 与 housekeeping 仍排除，查询失败不解释成“零结果”。

invalid_args 提示修正输入；not_found 不区分缺失/不可读/快照外；query_timeout 可同游标
稍后重试；cancelled 停止；provider_failed 表示读取能力失效。原账本、存储 schema、
recent/live-feed API 不变，只增加狭窄只读的 ID/reply 查询能力，业务解释收在 platform/home。

测试覆盖元数据误命中、中文双视图分段、快照替换、失败/合并状态、未知工具保留、
跨角色邻居去噪、关系去重与私有目标、返回动作合法性，以及实际 request/response 链路。
