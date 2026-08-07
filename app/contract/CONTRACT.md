# engine API 契约 v1（一页纸）

> v1.0（2026-08-05）。本页是壳作者面的契约正文；机器形 = 本目录的 method
> registry / 生成 schema（`testdata/engine-api.schema.json`，golden 零漂移闸
> 守护）与 `platform/subjectgate`（ws 帧信封）。可运行验收 =
> `scripts/demo-curl.sh`。形状与语义已冻结；生长恒走 §8。
> 设计推导与历史见 pm 仓 `coral/engine-api-contract.md`。

## 0. 你在跟什么说话

engine 是一个协作真相基座：**人、agent、设备在频道里协作**。频道是一条
durable log——消息 append 后不可变、不丢序，每条有频道内唯一且单调的 `seq`;
频道存续期间恒可分页回读（频道销毁后不再可读——durable ≠ 永久可查询）。
你的壳是人看这个世界的窗口：**壳只渲染和传导人的操作，恒不是执行主体**。

## 1. 契约面

- **`/api/*` + `/ws` 是全部契约面**。web、桌面、CLI、agent、curl 同权同门，
  engine 不知道也不关心你是谁家的壳。
- `/api/experimental/*`：明示会变，随时改删不算破坏。别依赖，或盯紧 CHANGELOG。
- `/compute`：内部线（设备接入），壳恒不碰。
- 壳恒直连，中间无代理层。

## 2. 认证

- 长期凭据恒走 `Authorization: Bearer <token>` 或同源 session cookie，**恒不入
  URL query**（唯一豁免：**单次 + 秒级 TTL + 绑定本次握手 + 兑换即失效**四条
  同时成立的 ticket，跨源 WS 场景用，当前未开；ticket 兑换响应恒
  `Cache-Control: no-store`，日志恒脱敏）。
- 同一 token 对 HTTP 与 WS 握手同形有效。本地自动化：`--init` 建户即发 token
  落数据目录（`atoll-token`，0600；落盘位置为发行细节非契约）。
- **版本发现**：`GET /api/meta` 返回 server 契约版本——用新字段/新端点前先看它
  （ws 侧 attach 回执带同一版本号）。

## 3. 读（重放）：HTTP 分页

- `GET /api/channels/:chID/messages?after_seq=N&limit=M` → `{messages, scanned_through_seq}`
  （参数细节以 registry/schema 为准；游标语义本身是规范）。
- 游标语义：**`scanned_through_seq` 是"替你扫到了第几号"**，按它推进游标。
  因可见性被滤掉的行对你表现为无缝——跳号恒不代表丢数据；
  丢没丢只看"你的游标 vs log 最新号"。
- **游标归你**：engine 恒不记你读到哪。本地存 per-channel last-seq 表，
  断线重连按表补页（点开哪个频道补哪个）。跨频道恒无聚合游标、恒无服务端未读数。

## 4. live：`/ws` 单链

- **一条连接 = 一个 principal（户）**。链上自动下发你全部**成员频道**的实时动态;
  哪个窗口开着是你的内政。中途入伙/被踢，流自动增减，无需重连。
  看非成员频道恒走 observe 帧（下），恒不自动下发。
- **信封（规范）**：每帧 `{v, frame_type, ref?, payload}`。`v` = 传输信封版本
  （与契约版本两回事）；`frame_type` = discriminator；**请求帧带 `ref`，
  receipt/error 回执原样回显 `ref`**——这是你关联请求与结果的唯一方式。
  注意别混淆三个"引用"：`ref` 是**帧信封**的回执分拣号（活到 gateway 为止，
  恒不落频道记录）；回复/引用某条消息用消息自己的 `parent_id`；对某条请求
  裁决/取消用 payload 里的 `req_id`。三者互不相干。
  **重发公式**：未收到回执就重发，恒 = 新 `ref` + 同消息 `id` + 同内容。
- **时序（规范）**：连上后你先发 `attach` 帧（带 ref）→ 收到 `receipt`（回显你的
  ref，payload 载 server 契约版本，与 `/api/meta` 同值）→ 进入双向流。
  失败 = `error`（回显同一 ref）。没有独立的 hello 帧。
- 帧类型（全表 + schema 见生成附件）：上行 `attach / submit / resolve /
  cancel / after / cancel_timer / resource / observe / unobserve`；下行 `feed /
  receipt / error / observe_ended`。下行 kind 会增长——不认识的恒忽略（生长律）。
- **feed 帧恒带 `(channel_id, seq)`**——live 流免费维护你的 last-seq 表;
  receipt / error / observe_ended 等控制帧不属任何频道 log，恒不推进游标。
- **observe / unobserve**：临时观看非成员频道（realm 公开观察）。连接局部，
  断线即清；资格逐帧活算，失效收 `observe_ended{channel_id, reason}` 帧。
- 可靠性：**live 流允许有损，但损恒显式**——你消费太慢时 engine 断链（这就是
  信号），重连后按 last-seq 表补页：**durable 事实零丢失，live-only 动画丢了不补**;
  **写路请求恒有确定性答复**（receipt 或 error，靠 ref 认领）。
- 相位事件 durable 可重放（`*.ended` 类事件带全值）。当前无流式 token 动画
  （engine 恒不下发 delta）；将来若提供，恒 live-only、断线不补——
  你恒不会丢事实。

## 5. 写（提交）

- 提交帧字段：`channel_id`、`id`（**由你铸造**，uuid 即可）、`msg_type`、
  `kind`、`audience`、`payload`。普通内容只放文本等内容字段；agent 动作由
  `msg_type` 表达（如 `agent.steer`、`agent.interrupt`）。
- `kind=event` 的 `audience` 可省略或为 `[]`：两者都规范化为 wire/store 的
  `[]`，表示只落账、不即时唤醒。`request` 省略 audience 时仅 human 接入口会按
  默认应答者补齐；最终 request/response audience 恒恰为一个具名 actor。
- **幂等**：幂等键 = `(channel_id, id)`，`id` 由你铸造。未收到回执恒重发即可：
  同键**同内容**重发 → 返回原回执（确认原 message id；receipt 恒不含 seq——
  位置由 feed/分页读到达）；同键**不同内容** → 错误码 `idempotency_conflict`
  （你复用了 id，恒不当成功）。**不带 `id` 恒无幂等**——要重试安全恒自铸。
- `agent.steer` 可在 payload 带 `expected_turn_id` 做显式 CAS；目标已失效会得到
  `cas_mismatch`。普通内容消息不带调度字段，由 agent 的合并策略决定何时生效。
- **slash 命令恒由 engine 解析**，你只透传文本 + 渲染结果。
- 人能在 UI 做的，agent 走同一协议能做——恒无 UI 专属旁路。

## 6. 活动流（过程渲染）

- agent 干活过程 = 频道 log 里的相位事件消息（`kind=event`，type 为
  `activity.*` 族：`activity.turn.started/ended`、`activity.tool.started/ended`
  ——词表见生成 schema，只增）；最终答案 = `kind=response` 消息，
  `correlation_id` 指回触发请求。**你只渲染，恒不自建过程状态机。**
- 不认识的 `activity.*` type 恒不渲染、恒不报错。

## 7. 错误形（两面各一种，已锁）

**REST 面**：`{code, message, details?, will_retry?}`。
- `message` 给人看，措辞会变，恒不解析；day-1 起有 `details` 位——恒不需要
  正则抓 message；
- `will_retry: true` = engine 会自动重试、turn 未中断，你不用弹错。

**WS 面**：`error` 帧，payload = `{frame, code, detail?}`——`frame` 回显你
出错的帧类型，请求关联靠外层信封的 `ref`（§4）。字段比 REST 少是有意的：
帧错误恒是"这一帧被拒"的机器裁决，没有面向用户的长文案位。

两面共同律：
- **恒按 `code` 分支**（各自闭集词表，只增不改，known-values 见生成 schema）;
- **不认识的 `code`**：恒当一般失败结算该请求——恒不忽略、恒不挂起。

## 8. 生长律（你要遵守的唯一纪律）

- **下行：忽略一切不认识的**——字段、帧 kind、activity kind、枚举值。
  engine 加东西恒不算破坏，你的壳恒不因 engine 升级而坏。
- **上行：只发你认识的**——未知字段会被拒收（结构化错）。用新字段前先看
  `/api/meta`（或 attach 回执）的契约版本。拼错字段名会当场 400，这是在帮你。
- 下行结构恒带名字段，恒无数组下标语义。
- 弃用恒带 removeAfter 时间窗，CHANGELOG 喊一声。

## 9. 最小闭环（可运行验收）

`scripts/demo-curl.sh`：注册 → Bearer token → `/api/meta` → 建频道 → 建并挂载
daemon（agent 恒设备宿主）→ 引入 echo 工具 + 脚本 agent → ws 提交（attach
回执带版本、自铸 id、普通内容 payload）→ agent 回应落 live → 分页读回验证。
纯 curl + 一个 ws 小工具（`scripts/demo/wssubmit`），全程 header 认证。
