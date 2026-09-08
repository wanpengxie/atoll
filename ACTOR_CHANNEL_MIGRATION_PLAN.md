# Actor Channel v3.3：线上只读盘点与迁移计划

## 执行更新：owner 授权先删除旧模板

已通过 `system.channel.template.delete` 将 `native-agent-channel` 标记为 revoked，并再次读取确认；模板记录保留，未删除身体、成员或账本。尝试删除 v4 旧 Seat 声明 `87ac52ac-4067-49d0-a2e0-3b84225ae9e4` 时，旧运行节点返回 reserved（system declaration is reserved），该声明仍为 present。未绕过保护修改数据库。`channel-handle` 在当前公开声明查询中不存在，它出现在身体 genesis 中，不能当作已注册模板删除。新的 Handle 创建仍需运行新版配置解析器。

2026-09-08。本文件只记录读取结果和待授权操作；未执行线上迁移、重启、删除成员或删除身体。配置凭据不收入本文件。

## 已读取的事实

来源：当前节点的 `system.channel.list {include_actor_channels:true}`、`system.actor.template.list`、`system.channel.template.get` 与 c0.dev 实时名册。名单是当前调用方可见范围，不声称包含已退役身体或所有频道的动态 overlay。

| 对象 | 当前事实 | 迁移要求 |
|---|---|---|
| pi-agent-native-v4 / `87ac52ac-4067-49d0-a2e0-3b84225ae9e4` | present，type=actor，serving=0，父为 c0.dev；genesis Handle 使用 `{body_channel,host_channel,receiver}` | 改为独立 Handle 声明及 `{host,words,drivers}` overlay，保留 body ID 与账本 |
| v4 的同名公开声明 | ID 直接为 body ID，class=channel-seat，singleton=true；配置同时含 H/A | 旧 ID 恢复为 peer 声明；另建 `seat:<body>`，只含 `{body}`；不得原地将正在使用的 Seat 成员变为 peer |
| c0.dev 的 v4 Seat | `channel:87ac52ac-4067-49d0-a2e0-3b84225ae9e4:1788855895325` 在场 | 授权维护窗口内先停用旧关系，按新 Seat 声明重新引入；保留原账本和身体 |
| pi-agent-native-v2 / `b74aae76-cda9-4d5d-91d4-d293be1e814b` | present，type=actor，serving=1；genesis 有 svcactor 和父 peer，无 Handle | 保留原服务关系；发布独立 Seat 声明。是否额外建立成员关系不能由旧 type 标签推断 |
| c0.dev 的 v2 peer | `peer:c0.dev.pi-agent-native-v2:1788837082769` 在场 | 保留；不把 peer 原地替换成 Seat |
| `native-agent-channel` 公共模板 | 含五个业务成员、host_actor=host；没有显式 Handle；profile.svc_agent=native-agent | 换为 docs/examples 的模板，显式引用 native-agent-handle，不再靠 svc_agent 产生 Handle |

当前可见公开声明中未发现新的 `seat:<body>` 声明；v4 是唯一可见 channel-seat 声明。读取的运行节点词表仍是旧模型，这次本地测试不代表线上已升级。

## 已准备的模板

- `docs/examples/native-agent-handle.json`：五个 agent 工作词的 schema、内部 target=native-agent、drivers 为 native-agent 和 agent-looper。host=c0 是声明默认值；actor 创建流程按实际父频道设置 overlay。
- `docs/examples/native-agent-channel.json`：保留五个业务成员并显式引入该 Handle；serving=0，不配置 svc_agent；不包含 API key。
- `TestNativeChannelRecipeUsesExplicitHandleAndCurrentWorkSchemas` 将样例 schema 与源码中的 Native Manifest 比较，防止模板漂移。

## 经授权后的执行顺序

1. 在维护窗口冻结新建/引入；备份 registry、相关 Home 状态和账本，记录校验值。精确读取 c0、c0.dev、v2、v4 的成员定义、所有相关 overlay 与 genesis，核实是否还有别处的 Seat。当前公开工具没有 overlay 读取和跨频道成员读取能力，不能用空结果代替这一步；需在节点 registry 权限下执行只读检查。
2. 输出逐行修改 dry-run：v4 旧声明引用的全部成员、v4 Handle genesis 与 overlay、两份 Seat 声明、旧 ID 的 peer 声明，以及模板；发现同协议重复关系则停下确认，不按先到先得丢成员。
3. 授权后停用 v4 旧关系，保留身体与全部历史。更新注册声明及配置，并使用既有成员删除/引入流程更换 Seat/Handle；不 SQL 改写历史消息身份，不把旧 Seat 原地变为 peer。迁移不能在新旧不兼容解析器之间边服务边改，应在维护窗口统一完成。
4. v4 恢复 c0 控制 peer、c0.dev 新 Seat；v2 原 peer 保留，只发布可选 Seat 声明。移除已失效的旧配置 overlay，避免覆盖新的声明。公共模板切换到本仓库样例。业务凭据沿用原处，不复制到新模板。
5. 再启动新二进制。以正常消息验证双向 call/event、H 账 sender=Seat、取消与移除撤权。检查 v2 的普通 peer 服务保持原行为。
6. 失败时停止继续引入，根据备份恢复配置和旧二进制；已追加账本不能回滚或删除。给出已完成/未完成操作清单后再决定重试，禁止重建身体冒充恢复。

Access door 挂载、Space 身体不在本轮设计范围。迁移执行等待 owner 授权，不能用本地验收替代线上验收。
