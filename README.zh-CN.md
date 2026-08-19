# Atoll

[English](README.md) | 简体中文

**一个面向 agent 协作的内核。**

Atoll 是一个自托管的节点：你的 coding agent（codex、claude……）、MCP 工具和你本人，在共享的
**频道（channel）** 里一起干活。频道里所有人说的每一句话都进同一份由 server 持有的只追加日志；
"谁说的"是被强制保证的，不是自己填的；一个 actor 能碰什么，由频道成员身份决定，而不是每个
工具各自的配置。你把它跑在自己机器上，从浏览器或脚本跟它说话，并且可以在运行时往里加
agent、工具和机器——不用重新编译。

它是内核优先的：保证来自结构，不来自 prompt。背后的推理见
[docs/architecture](docs/architecture/README.md)；本 README 只讲怎么装、怎么用。

> **状态：v0.01 —— pre-release，不对数据负责。**
> Atoll 今天已经能端到端跑起来（`atoll up` + web UI），但它是一个早期内核，不是产品。
> **存储格式、API 和 wire 协议会随时改，没有 deprecation 周期、没有迁移路径**——新版本可能
> 拒绝打开、或直接覆盖旧版本建的 node home。不要往里放任何你无法重建的东西。见 [状态](#状态)。

---

- [快速开始](#快速开始)
  - [1. 编译](#1-编译)
  - [2. 安装并启动一个节点](#2-安装并启动一个节点)
  - [3. 启动 web UI](#3-启动-web-ui)
  - [把角色拆成独立进程](#把角色拆成独立进程)
- [你拿到了什么](#你拿到了什么)
- [使用节点](#使用节点)
  - [从 web UI](#从-web-ui)
  - [从脚本](#从脚本)
  - [常见操作](#常见操作)
- [扩展：写你自己的 actor](#扩展写你自己的-actor)
- [会遇到的几个概念](#会遇到的几个概念)
- [仓库布局](#仓库布局)
- [开发](#开发)
- [文档](#文档)
- [状态](#状态)
- [许可证](#许可证)

---

## 快速开始

一套完整的本地环境三步：编译二进制、跑一次安装器、对着节点起 web UI。结果是一个跑在
`http://127.0.0.1:8832` 的节点、一个 `root` 账号，以及一个已经坐在根频道 `c0` 里、作为它
**steward** 的 coding agent（codex 或 claude）——打开 UI、登录、直接跟它说话。

**前置条件**

| 用途 | 需要 |
|---|---|
| 节点 | Go 1.25+（见 [go.mod](go.mod)）、`make`、`curl` |
| steward agent | [`codex`](https://github.com/openai/codex) 和/或 [`claude`](https://github.com/anthropics/claude-code) CLI 已安装**且已登录**（安装器会检测两者并让你选；也可以之后再加） |
| web UI | Node.js 22+ 和兄弟仓库 [`atoll-web`](https://github.com/wanpengxie/atoll-web) |

### 1. 编译

```bash
git clone git@github.com:wanpengxie/atoll.git
cd atoll
make build            # -> bin/atoll, bin/atoll-server, bin/atoll-daemon
```

### 2. 安装并启动一个节点

第一次——交互式安装器。它做预检（之前的安装、codex/claude CLI 及其登录状态、端口是否空闲、
home 是否可写），让你选 c0 的 steward，设 root 密码，写 `<home>/atoll.env`，然后跑
`atoll up` 并打印登录方式：

```bash
scripts/install.sh
```

非交互（全部取默认值；适合 CI / 脚本化）：

```bash
scripts/install.sh --yes
# 识别 ATOLL_HOME / ATOLL_ADDR / ATOLL_ROOT_PASSWORD / ATOLL_STEWARD
```

之后每次启动只需节点本身——它读安装器写下的 `atoll.env`，所以裸跑 `atoll up` 就是重开
同一个实例（默认 home `~/.atoll`）：

```bash
bin/atoll up                      # 等价于 bin/atoll up --dir ~/.atoll
bin/atoll up --dir ~/.atoll --addr 127.0.0.1:8832   # 显式 flag 仍然优先于 atoll.env
```

`atoll up` 是一个进程里的整个节点：安装或打开 `c0`，刻出 root 账号和 home 频道，绑定监听，
再连上那台 well-known 的本地 device（真正跑 agent 和工具的计算宿主）。你不需要手动造
device 或挂 device。

安装器留下的东西：

```
~/.atoll/
├── atoll.env                 # ATOLL_ADDR / ATOLL_STEWARD —— 这次安装的记忆
├── atoll-up.log              # 节点日志（JSON lines）
├── server/
│   ├── atoll-token           # 本地自动化用的 bearer token（0600）
│   ├── root-password         # 如果你设了/生成了密码（0600）
│   ├── registry.db           # space 注册表
│   └── channels/             # 每个频道一个 SQLite 真相日志
└── device/                   # 本地 device 的身份 + 工作目录
```

看它活着没有：

```bash
curl -s http://127.0.0.1:8832/healthz      # {"status":"ok"}
```

账号：`root@atoll.local`，密码是你设的那个（或 `~/.atoll/server/root-password` 里生成的那个）。
`Ctrl-C` 停止；节点是单 home 的，对同一个 `--dir` 再跑一次 `atoll up` 会被锁拒绝。

### 3. 启动 web UI

浏览器客户端是一个独立仓库，**不由节点托管**。它是一个 Vite 应用，把 `/api`、`/ws`、`/obs`
和 `/files` 代理到节点，所以浏览器始终只看到一个源，普通的 cookie 登录就能用。

```bash
# 在兄弟目录里，第 2 步的节点已在 :8832 跑着
git clone git@github.com:wanpengxie/atoll-web.git
cd atoll-web
npm install
npm run dev                   # -> http://localhost:5173
```

节点如果监听在别的地址，把开发代理指过去（只影响代理，不会进浏览器 bundle）：

```bash
ATOLL_SERVER_URL=http://127.0.0.1:9000 npm run dev
```

打开打印出来的地址，用 `root@atoll.local` 登录，选 `c0`，在编辑器里 `@steward …`——回复以
一轮一轮（round）的形式流回时间线。`atoll-web` 还自带一个同契约的独立 mock（`npm run mock`），
想不起节点先玩玩 UI 可以用；见它的 README。

三者在一台机器上装起来长这样：

```
 browser ──► atoll-web (vite :5173) ──proxy──► atoll up (:8832)
                                               ├── server   （真相：c0、频道、注册表）
                                               └── local device（跑 codex/claude/工具）
```

### 把角色拆成独立进程

`atoll up` 是两个二进制的糖衣，它们共用同样的磁盘 home，所以用 `atoll up` 起的节点以后拆开
不用任何迁移：

```bash
# server —— 持有所有频道的真相；首次运行自装（生成的 root 密码打到日志里）
bin/atoll-server --home ~/.atoll/server --addr 127.0.0.1:8832

# daemon —— 一台计算宿主；首次用 device key 绑定（由 `atoll up` 的 provisioning 生成，
# 或通过 system.device.create 这个 space 词生成）；之后裸启动，身份保存在 home 里
bin/atoll-daemon --home ~/.atoll/device --server "ws://127.0.0.1:8832/compute" --key <device-key>
```

只有真要把角色放到不同机器上时才需要这样。

## 你拿到了什么

`atoll up` 之后，你手里具体有：

- **一个节点**，在 `127.0.0.1:8832`，持有一个 space。真相以 SQLite 文件形式在
  `~/.atoll/server/`；没有只存在内存里的东西。
- **一个根频道 `c0`** 和它下面的 `lobby`。`c0` 是管理 space 的地方——新频道、agent、
  device、账号，全部是在 `c0` 里对 `system` 说话造出来的。
- **一个账号** `root@atoll.local`（唯一的一个，直到你再建或用 `--open-registration` 开放注册）。
- **一个 steward**——你选的那个 codex 或 claude CLI，作为 agent actor 跑在 `c0` 里、跑在你的
  机器上、用你自己的 CLI 登录。@ 它，它就去干活，把每一轮流进频道，并且能调用挂在它旁边
  的工具。
- **一台本地 device**——真正跑 agent 和工具的计算宿主。自动挂上；更多机器可以作为额外
  device 加入。
- **一个 bearer token**，在 `~/.atoll/server/atoll-token`，让同一台机器上的脚本不用登录就能
  跟节点说话。

频道成员说的每句话、做的每件事，都是这个频道里的一份有序日志。web UI、agent 和你的脚本读的
是同一份日志、写的是同一道门；谁都没有特权后门。

## 使用节点

### 从 web UI

`atoll-web`（上面第 3 步）是一个朴素的聊天式客户端，走的是节点的公开契约：

- **频道**在左侧：`c0`、`lobby`，以及你建的。每个频道显示时间线（人的消息、agent 的回合、
  系统事件）和名册。
- **跟 agent 说话**：在编辑器里 `@` 它。回答以一个回合（round）到达——queued → processing →
  工具调用 → 最终文本——这些全是日志里的消息，所以你能看到它到底做了什么。
- **审批**：agent 要一个需要人的东西（凭证、有风险的动作）时，频道里出现一张卡；在卡上批准或拒绝。
- **名册**：频道里有谁——人、agent、`system`、工具——以及每个能被问什么（`actor.describe`）。

UI 能做的，脚本都能做；它用的就是下面这同样的三张面。

### 从脚本

用本地 token 认证：

```bash
TOKEN=$(cat ~/.atoll/server/atoll-token)
AUTH="Authorization: Bearer $TOKEN"
```

节点恰好只暴露三张面：

| 面 | 方法 | 是什么 |
|---|---|---|
| `/obs/...` | `GET` | 对 space 和频道的只读观察 |
| `/ws` | WebSocket | 往频道里送任何东西的唯一通道 |
| `/files/<address>?t=<ticket>` | `GET` / `PUT` | 文件数据面；ticket 通过 `/ws` 签发 |

**读**，每个一条 curl：

```bash
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/channels
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/daemons      # device
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/principals   # 账号
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/decls        # actor 模板
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/channel/c0/profile
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/channel/c0/actors
```

返回是 `{subject, kind, complete, items[]}`；每个 item 把账本上写的（`declared`）和刚观察到的
（`actual`）并排给出。观察不到的事实返回 `unknown` 并带原因，绝不编一个 `false`。

**写**走 WebSocket：用同一个 bearer token 打开，先发一帧 `attach`，之后每个请求一帧 `submit`。
回执和其他所有成员的消息从同一条连接回来，所以整个会话保持连接不断。

```jsonc
// 1. 订阅（从头回放；传游标则续传）
{"v":2,"frame_type":"attach","ref":"a","payload":{"since":{}}}

// 2. 在 c0 里让 `system` 列出成员
{"v":2,"frame_type":"submit","ref":"r1","payload":{
  "channel_id":"c0","msg_type":"system.member.list","kind":"request",
  "visibility":"public","audience":["system"],"payload":{"body":{}}}}
```

回执带 `payload.message_id`；答复随后以 `feed` 帧到达，`kind:"response"`、`parent_id` 等于那个
id、终态 `status` 为 `completed` 或 `failed`。要像 web UI 那样跟 agent 说话，提交
`msg_type:"human.text"` 并把 agent 放进 `audience`；它的回合（`activity.turn.*`、
`activity.tool.*`）和最终答复都从 feed 回来。

**管理是一套词表，不是一个管理面板。** 全部是对 `system` 的请求（模板类的词发给
`system:registrar`）：

| 族 | 词 | 在哪说 |
|---|---|---|
| 频道 | `system.channel.create/get/list/set/delete`、`system.channel.template.*` | `c0` |
| agent 与工具（class） | `system.actor.template.create/get/list/set/delete`、`system.actor.overlay.set/delete` | `c0` |
| 成员 | `system.member.create/get/list/delete/restart/admit` | 目标频道本身 |
| 机器 | `system.device.create/list/attach/detach/delete` | `c0` |
| 账号 | `system.principal.create/get/list/delete` | `c0` |
| 凭证 | `system.credential.set` | 目标频道本身 |
| 日志 | `system.log.recent` | 目标频道本身 |

完整的请求/响应形状就是 [`protocol/message/system.go`](protocol/message/system.go) 和
[`platform/lagoon/contracts.go`](platform/lagoon/contracts.go) 里的 Go struct。

### 常见操作

**往频道里加第二个 agent。** agent 是*模板*（一个 class——`codex`、`claude`、`script`——加配置），
然后*坐进*某个频道：

```jsonc
// 在 c0 里，发给 system:registrar —— 声明模板
{"msg_type":"system.actor.template.create","payload":{"body":{
  "id":"reviewer","name":"Reviewer","class":"claude","visibility":"private",
  "singleton":false,"config":{/* 各 class 自己的配置 */}}}}

// 在目标频道里，发给 system —— 坐进一个实例
{"msg_type":"system.member.create","payload":{"body":{"decl_id":"reviewer"}}}
```

**挂一个 MCP server——两条消息，不用重编。**

```jsonc
// 在 c0 里，发给 system:registrar
{"id":"my-mcp","name":"My MCP","class":"mcp","visibility":"private",
 "singleton":false,
 "config":{"name":"testsrv","transport":"http","url":"http://127.0.0.1:8931/mcp"}}

// 在频道里，发给 system   -> {"status":"completed","member":"tool:my-mcp:<ts>"}
{"decl_id":"my-mcp"}
```

这个 actor 出生时就连上 server 并发现它的 `tools/list`；每个工具变成一个以你声明的 `name`
为前缀的消息类型（`testsrv.echo`、`testsrv.add`……），对它 `actor.describe` 能看到。本地子进程
用 `transport:"stdio"` 加 `command` / `args` / `cwd` 代替 `url`。同频道里的 agent 可以调这些工具。

**把另一台机器接成 device。** 在 `c0` 里用 `system.device.create` 要一把 key，然后在另一台机器上：

```bash
bin/atoll-daemon --home ~/.atoll/device --server "ws://<node>:8832/compute" --key <device-key>
```

它会出现在 `/obs/space/daemons` 里，并像本地那台一样能承载 agent 和工具。

## 扩展：写你自己的 actor

频道里住着的任何东西——agent 引擎、工具、适配器——都是一个 **actor**：一个建立在一小张动词表
上的 Go 函数。`Sys` 在出生时交给它，是它碰世界的唯一途径——收是 `Recv`，答是 `Reply` 或
`Fail`。发送者身份是焊死的，不是 actor 自己填的字段。下面是真实的 echo actor，一字不少：

```go
// drivers/tools/echo/echo.go
func run(sys actorbase.Sys) error {
    for {
        msg, err := sys.Recv()
        if err != nil {
            return err
        }
        switch msg.Type {
        case "echo.say":
            _, _ = sys.Reply(msg, msg.Payload)
        default:
            _, _ = sys.Fail(msg, "type_unsupported", "echo does not handle "+msg.Type)
        }
    }
}
```

```go
// drivers/tools/echo/register.go —— 一条注册表条目：config → 运行中的 actor
func init() { registry.Register("echo", registry.ClassDecl{Kind: actor.KindTool, New: construct}) }

func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
    return platform.ActorDecl{
        ID:      spec.ID,
        Kind:    actor.KindTool,
        Factory: platform.ActorFactory{Proc: actorbase.Def{
            Doc: "echoes echo.say back",
            New: func() (actorbase.Proc, error) { return run, nil },
        }},
    }, nil
}
```

像这样注册一个 class，重编，然后 `system.actor.template.create` 带 `"class":"echo"` 就能让每个
频道用上它。自带的引擎（`codex`、`claude`、`script`）和工具（`echo`、`mcp`、`device`、`kimi`、
`xhs`）都是这么在 `drivers/` 下造出来的；逐步的演练见
[docs/architecture/09-actor-hello-world.md](docs/architecture/09-actor-hello-world.md)。

## 会遇到的几个概念

五个词覆盖整个系统；其余一切由它们搭出来。

| 词 | 意思 |
|---|---|
| **channel（频道）** | 一个房间：一份日志、一套成员、一组文件。`c0` 是根；其它频道在它下面建。 |
| **actor** | 任何是频道成员、能说话的东西：人、agent、工具、`system`。用稳定 id 寻址；每次运行是一个独立的 incarnation。 |
| **message（消息）** | 频道日志里的一条。请求恰好得到一个终态响应——来自 actor、来自超时、或来自 actor 死亡。 |
| **access（访问）** | actor 去碰一个文件、一个密钥、一块状态。由拥有它的频道的成员身份放行；不记日志，但有门。 |
| **device** | 为节点跑 actor 的一台机器。`atoll up` 给你一台；`atoll-daemon` 再加。 |

你在上面搭东西时可以依赖的几条节点保证：

- **server 是唯一的写者。** 每条消息先追加进频道日志，然后别人才能看到；没有旁路，没有 REST "send"。
- **发送者不可伪造。** actor 用出生时铸的笔写字。
- **成员身份就是权限。** 频道内成员读写平等；没有按对象的 ACL 可管、也就没有可漂移的。
- **每次写都过同一条准入链**——调用者授权、信封形状、发送者一致性、kind/audience 规则、响应配对、去重。
- **死掉的 incarnation 不能缠着日志**；重启的 agent 是同一身份的新 incarnation。
- **本地与远端 actor 对运行时不可区分。**
- **分层由机器检查**（`archtest/`）——对 agent 写的代码同样成立。

为什么长成这样、它和 MCP、A2A、消息总线的关系，是[架构系列](docs/architecture/README.md)的主题。

## 仓库布局

```
cmd/         二进制：atoll（节点）、server、daemon；+ devtools 与共享 internals
scripts/     install.sh（交互式安装器）与仓库 lint 脚本
drivers/     外部世界驱动：tools/（echo、device、kimi、mcp、xhs）、
             agents/（agent 引擎 provider：codex、claude、script）、
             gateway/（人的入口；portal/ = 身份门 + /ws + /obs + /compute）、
             devicehost/（`atoll up` 连上的本地 device）
protocol/    协议类型（envelope、actor、channel、access、resource、system 词表）
runtime/     内核运行时（准入流水线、actor cell/port、sqlite store、
             actor store、schedule/timers、access door）
lib/         给 actor 作者的标准库：actorbase（每个 actor 都站在上面的 Proc + 动词表基座）、
             behavior、metatool（工具目录）、introspect
platform/    跨宿主装配：channelhost/、peeractor/ + svcactor/（跨频道路径）、home/（server
             侧频道 home）、daemonhost/ + compute/（device 侧）、subjectgate/、
             lagoon/（注册表 + registrar）、boot/（安装器）
registry/    actor class 注册表（config → 运行中的 actor）
archtest/    架构约束测试（分层图 + 闭集）
e2e/         对真实 server + daemon 二进制的端到端测试
mcp-testserver/  测试和 MCP 示例用的小 MCP server
docs/        架构系列、开发演练、产品笔记
atoll-site/  项目站点（Jekyll）
```

## 开发

```bash
make build          # 三个二进制进 bin/
make test           # 日常档：-short -race，sqlite fsync 关（tag atolltestfast）
make test-full      # 合 main 前的门：同上，但一个不跳
make lint           # go vet + 架构测试 + 仓库 lint 脚本
make e2e-loop       # 黑盒验收：两个真实 OS 进程走 portal wire
make dev            # 只起 API server 在 :8832，home 在 /tmp/atoll-dev（做 atoll-web 时用）
```

测试按包组织；改哪个包就跑哪个包（`go test -race ./runtime/actorrt/`），不要整棵树跑。
`archtest/` 里的架构测试是 `make lint` 的一部分，设计上就是分层不变量被破坏时要红——改它之前
先读那条测试的头注。

## 文档

- [docs/architecture/](docs/architecture/README.md) —— 为什么是内核而不是 agent loop、五元素、
  频道、actor、调度、联邦、路线图，以及一个 actor hello-world。
- [docs/dev/](docs/dev/README.md) —— 开发者演练：substrate、actorbase、组合，以及当期施工背后的设计笔记。
- [docs/production/](docs/production/README.md) —— 定位、竞品笔记、采用策略。
- [docs/credential-system-walkthrough.md](docs/credential-system-walkthrough.md) —— 凭证如何在访问面里流动。
- [`atoll-web`](https://github.com/wanpengxie/atoll-web) —— 浏览器客户端及其契约笔记（含一个节点的独立 mock）。

## 状态

Atoll 是 **v0.01 —— pre-release**。它能端到端跑：一条命令装好并启动节点，一个 coding agent
坐在 `c0` 里当 steward，MCP server 运行时挂载，web UI 走公开契约跟它对话。

这个版本号在实践中意味着：

- **不对数据负责。** 1.0 之前没有升级或迁移路径。SQLite schema、注册表、磁盘 home、wire
  方言都会变；新二进制可能拒绝旧 home，或者悄悄把它重刻一遍。把 `~/.atoll` 当一次性的——
  安装器之所以提供"把旧 home 挪开"正是为此。
- **API 变动没有 deprecation 周期。** 消息类型、帧形状、Go 包表面都随内核评审修订；没有任何东西冻结。
- **当前边界：** 一个节点一个信任域；`atoll up` 只替你管本地那一台 device（其它手动挂）；
  自带的引擎是 codex、claude 和一个通用的 `script` 运行器。

内核优先，打磨其次——更多连接器、脚手架、打包在后面。想看后续就 watch 这个仓库。

## 许可证

[Apache-2.0](LICENSE)。Atoll 名称及任何托管服务与代码许可证是两回事。

## 名字

项目叫 **atoll**（小写）；Go module 是 `github.com/wanpengxie/atoll`。atoll（环礁）是无数微小
生物一层一层沉积而成的珊瑚环——没有谁拥有这片礁，它靠沉积生长。这就是设计。
