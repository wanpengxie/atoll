# Bug:claude 派出子 agent 后,进度和结果都丢

- 日期:2026-09-08
- 状态:已定位,未修
- 范围:`drivers/agents/provider/claude`(claude 驱动);前端无责

## 1. 现象

owner 观察:"一旦 claude spawn agent 去干活,我就会丢失整个进度。"

实际丢的是两层,第二层比第一层严重:

1. **子 agent 干活期间,时间线上只剩一行 `tool: Agent 开始`**,之后几分钟空白。子 agent 读了哪些文件、说了什么,一条都不在账上。
2. **子 agent 在后台完成后,claude 接着写出的整段结果根本没进频道。**owner 从来没收到那条消息,只看到之前一条"等它回来我再合成"的短回复就终了了。

## 2. 账本证据(c0.dev,head_seq 32436)

Pi 对比那一题:

| seq | 内容 | 说明 |
|---|---|---|
| 32140 | root → claude `agent.ask`:"对比分析下这个分支实现的agent,和pi agent的区别" | 请求 |
| (中间) | claude 的 progress 行:`tool: Agent` started(description "Inventory Pi agent capabilities") | 子 agent 派出 |
| 32155 | claude 终态 `completed`,text = "分支这边读完了…Pi 那一侧的能力清单还在由只读 agent 盘点,等它回来我把两边合成对比。" | **回合在这里关了**,时间 14:43:35 |
| — | 子 agent 约 14:47 完成,claude 随即写了 1000 多字的对比("对比做完了。先给定位…") | **账本上不存在这条**。按 participant=claude 搜"对比做完了",最近 512 行内零命中 |

同一模式第二次:配置模型那一题,claude 派出"Inventory atoll config surfaces"子 agent 后以"只差代码侧的配置入口清单…回来后我把文档写出来"终了;子 agent 完成后写出的东西同样没有回合可挂。

## 3. 根因

两处都在 claude 驱动的输出处理里,是同一个假设的两个后果:**驱动只认"当前有一个活动回合"这一种状态**。

### 3.1 子 agent 的帧被整体丢弃

[output.go:157](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/provider/claude/output.go:157)

```go
func (w *worker) onFrame(c *connection, typ, subtype string, raw json.RawMessage) {
	var parent parentFrame
	if json.Unmarshal(raw, &parent) == nil && nonNull(parent.ParentToolUseID) {
		return
	}
```

Claude Code 的 stream-json 里,子 agent 产生的每一帧(assistant / user / tool_use / tool_result)都带 `parent_tool_use_id` 指向父回合里那个 `Agent` 工具调用。驱动在入口处把它们全部 `return`,所以子 agent 的活动既不成 progress 行,也不算 Activity 心跳。这是现象 1。

### 3.2 后台任务完成后的输出没有回合可挂

CLI 侧的时序:

1. claude 调 `Agent` 工具,`run_in_background: true`,工具立即返回一个 task id;
2. claude 结束本回合的文字 → CLI 发 `result` 帧 → 驱动 `onResult` 结算回合,`agent.ask` 写终态(seq 32155);
3. 子 agent 完成 → CLI 向同一个会话注入一条合成的 user 消息(task-notification)→ claude 继续工作、写出最终结果 → CLI 发 `assistant` 帧和又一个 `result` 帧;
4. 驱动此时 `w.turn == nil`,于是:

- [output.go:321](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/provider/claude/output.go:321) `onAssistant` → `activeTarget` 失败 → `w.unsolicited("assistant")`,**文本块直接丢**;
- [output.go:409](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/provider/claude/output.go:409) `onResult` → `w.turn == nil` → `w.unsolicited("result")`。

`unsolicited` 只发一条 `Diagnostic{Code:"unsolicited_cycle"}`,第一次 warn、之后 debug([output.go:564](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/provider/claude/output.go:564));引擎把 Diagnostic 一律记 debug 级([engine.go:556](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/runtime/engine.go:556))。节点日志跑 info,所以 `atoll-up.log` 里连一行痕迹都没有。这是现象 2,也是为什么"看日志"看不出来。

顺带:CLI 其实在明确通报这件事——每次后台任务集合变化都发 `background_tasks_changed` 帧,驱动把它当噪音([output.go:199](/home/xiewanpeng/.atoll/device/daemons/local-device/channels/c0.dev/atoll/drivers/agents/provider/claude/output.go:199))。

### 3.3 为什么不是前端

前端只投影账本。3.1 的帧没进账本,3.2 的文本没进账本,前端无从显示。`@我` 范围、housekeeping 排除、折叠都与此无关。

## 4. 影响

- 凡 claude 用 `Agent` 工具做后台任务(读大量文件、并行调研),owner 看到的是一条"我去派人了"的短回复,随后结果静默蒸发。claude 自己的上下文里结果是在的,它以为已经交付了。
- 前台 `Agent` 调用(`run_in_background: false`)只有现象 1:结果能回来,但过程是黑的,`ControlFactDeadline`(45s)期间没有 Activity,可能被判成无响应。
- 与 timer wake 那条线对照:闹钟到点时驱动特意 Post 一条自我委托,就是为了让"没有人问却要说话"的输出有回合可挂(`timerwake.go` 的注释写得很清楚)。后台任务完成是同一类事件,目前没有对应的委托。

## 5. 修复方向

按小到大:

1. **诊断升级。**`unsolicited_cycle` 至少 warn 级并带丢弃的文本摘要;否则下次还是查不出。一行改动。
2. **子 agent 帧不丢,投影成嵌套活动。**3.1 处不再 `return`,而是把带 `parent_tool_use_id` 的 tool_use / text 以 `Tool{...}` / `ProgressNote{...}` 发出,加一个 `ParentCallID` 字段。前端 `ProgressTrail` 认不认父 id 都能先显示;认了可以缩进。
3. **后台任务完成要有回合。**两种做法,选一:
   - **驱动侧自我委托**(与 timer wake 同形):驱动收到 `background_tasks_changed` 记下有后台任务;回合结算时若后台集合非空,不算真正结束,或在下一批 `assistant` 帧到来而 `w.turn == nil` 时 Post 一条自我委托请求(如 `agent.background.resume`),把这些帧挂上去。审计上诚实:账本能看出"这是子 agent 回来后的续篇"。
   - **回合不关**:后台集合非空时把 `result` 帧当作"阶段终态",保持回合 processing,直到集合清空再结算。简单,但一个回合可能挂很久,steer / interrupt 语义要重新核。
   建议前者。
4. **agent 侧临时规避(不改代码):**claude 不派后台子 agent;确需并行调研时用前台 `Agent`(结果能回来,只是过程黑),或子 agent 完成后先把结果写成文件,再给自己挂一个 timer,让 wake 回合来通报。

## 6. 复现

任一 claude 回合里执行 `Agent` 工具且 `run_in_background: true`,回合内不等待直接结束;子 agent 完成后 claude 的后续输出在账本上不存在。查 `system.log.query` 按 participant 搜后续输出的文本即可确认。
