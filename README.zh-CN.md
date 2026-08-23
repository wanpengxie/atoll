# Atoll

[English](README.md) | 简体中文

**一个给 agent 用的操作系统。** 装在一台机器上；agent（codex、claude、你自己写的）、工具
（MCP server、设备）和人就在上面并排跑在共享的频道里，每一个都有身份、权限、一份发生过什么的
持久账本，以及以后被叫醒的办法。Linux 给程序的是进程、文件、权限；Atoll 给 agent 的是
**actor**、**channel（频道）** 和 **成员身份**。

| Unix | Atoll | |
|---|---|---|
| 进程 | **actor** | 人、agent、工具、系统——同一种身份 |
| 文件系统 + 管道 | **channel** | 干活的地方，同时是账本——发生的一切都是它有序日志里的一条消息 |
| 字节流 | **message** | 请求 / 事件 / 响应；操作系统不读内容 |
| 文件 | **resource** | 文件、KV、外部对象；控制面在这里，数据面委托出去 |
| 权限位 | **access** | 频道成员身份决定谁能做什么 |
| cron | **timer** | 持久的叫醒；重启不丢 |

> **v0.01 —— pre-release，不对数据负责。** 存储格式、API、wire 协议随时改，没有 deprecation、
> 没有迁移；新版本可能拒绝或覆盖旧的 node home。不要往里放任何你无法重建的东西。

## 特性

- **一条命令到一个跑着的节点**——`atoll up` 装好 `c0`、root 账号、一台本地计算 device，并把一个
  coding agent（codex 或 claude）坐进去当 steward。
- **每条消息都在账上**——每个频道一本只追加的 SQLite 账；server 是唯一写者；发送者身份由运行时
  盖章，不由发送者填。审计、回放、恢复都从账里来。
- **频道是一切的单位**——信任边界、上下文、文件、生命周期；一棵按名字寻址的树（`c0`、`c0.dev`、`c0.alice`）。
- **成员身份即权限**——没有按对象的 ACL；agent 做的事就是它 principal 做的事。
- **agent 是成员，不是会话**——重启、换模型身份不变；`codex`、`claude`、`script` 三种引擎；一个模板多个实例。
- **工具运行时挂载**——一个 MCP server 两条消息变成成员，它的工具变成消息类型。
- **机器以 device 身份加入**——另一台机器上跑 `atoll-daemon`，就能承载有真实 shell、文件、git 的 agent 和工具。
- **声明式收敛**——账本上的期望状态对宿主证词；崩溃重启后收敛回去，缺席不杀任何东西。
- **三张面，没有后门**——`/ws` 帧写、`/obs` 读、`/files` 数据；web UI 和脚本用的完全是同三张。
- **分层由机器检查**——`archtest/` 在越层时让构建失败；对 agent 写的代码同样生效。

## 今天能做什么（2026-08）

| | |
|---|---|
| 安装、启动、登录、跟 steward 对话、web UI、脚本、挂 MCP、加 device、多频道 / 多 agent / 多账号 | ✅ |
| agent 在自己的模型循环里调用 Atoll 的工具（`call_actor` 等） | 🚧 工具口已有；codex / claude worker 还没暴露给模型 |
| 给 agent 程序化注入 prompt（我是谁、在哪、谁在场） | 🚧 已设计 |
| job、审批、额度、消息改道（组织层） | 📐 只有设计 |
| 跨节点联邦、DID 身份 | 📐 只有方向 |

---

- [快速开始](#快速开始)
- [一个节点由什么组成](#一个节点由什么组成)
- [使用节点](#使用节点) · [web UI](#从-web-ui) · [脚本](#从脚本) · [常见操作](#常见操作)
- [设计要点](#设计要点)
- [扩展：写你自己的 actor](#扩展写你自己的-actor)
- [仓库布局](#仓库布局) · [开发](#开发) · [文档](#文档)
- [状态](#状态) · [许可证](#许可证)

---

## 快速开始

结果是一个在 `http://127.0.0.1:8832` 的节点、一个 `root` 账号、一个坐在根频道 `c0` 里当
**steward** 的 coding agent，以及一个打开就能用的 web UI。

### 装一个发行版（最省事）

不用 Go、不用编译、不用单独起前端。这条命令按你的系统和架构取对应的二进制（UI 已经在里面），
校验 sha256，然后进入和源码安装**完全同一个**交互向导：

```bash
curl -fsSL https://raw.githubusercontent.com/wanpengxie/atoll/main/scripts/install.sh | bash
```

装完打开 `http://127.0.0.1:8832`。

```bash
ATOLL_VERSION=v0.01 ...        # 固定某个版本，不取最新
ATOLL_INSTALL_DIR=/usr/local/bin ...   # 换二进制装到哪（默认 ~/.local/bin）
```

包和 `checksums.txt` 都在 [Releases](https://github.com/wanpengxie/atoll/releases)；
mac 分 Apple Silicon / Intel 两个，Linux 分 amd64 / arm64，脚本按 `uname` 自己选。

steward 用的 codex / claude CLI 仍然要你自己装好并登录 —— 那是你的账号，装机脚本不替你登。

下面是**从源码**的路径。

**前置条件**

| 用途 | 需要 |
|---|---|
| 节点 | Go 1.25+（见 [go.mod](go.mod)）、`make`、`curl` |
| steward agent | [`codex`](https://github.com/openai/codex) 和/或 [`claude`](https://github.com/anthropics/claude-code) CLI 已安装**且已登录**（安装器会检测两者并让你选；也可以之后再加） |
| web UI | Node.js 22+（`make web` 会按 [WEB_VERSION](WEB_VERSION) 取 [`atoll-web`](https://github.com/wanpengxie/atoll-web) 编进二进制）；不编也能跑节点，只是 UI 是张占位页 |

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

安装器留下的东西：

```
~/.atoll/
├── atoll.env                 # ATOLL_ADDR / ATOLL_STEWARD —— 这次安装的记忆
├── atoll-up.log              # 节点日志（JSON lines）
├── server/
│   ├── atoll-token           # 本地自动化用的 bearer token（0600）
│   ├── root-password         # 如果你设了/生成了密码（0600）
│   ├── registry.db           # space 注册表
│   └── channels/             # 每个频道一本 SQLite 账
└── device/                   # 本地 device 的身份 + 工作目录
```

看它活着没有：

```bash
curl -s http://127.0.0.1:8832/healthz      # {"status":"ok"}
```

账号 `root`（写全 `root@atoll.local` 也行），密码是你设的那个（或 `~/.atoll/server/root-password` 里生成的那个）。
`Ctrl-C` 停止；节点是单 home 的，对同一个 `--dir` 再跑一次 `atoll up` 会被锁拒绝。

### 3. 打开 web UI

浏览器界面就在节点里，和 API 同一个端口：打开 `http://127.0.0.1:8832`。UI 用相对路径访问
`/api`、`/ws`、`/obs`、`/files`，和它们同源，所以普通 cookie 登录就能用，没有代理、没有跨域。

用 `root` 登录（节点自己的账号可以不写域），你就在 `c0` 里、steward 坐在对面；@ 它，回答以一个回合（round）
流回时间线。

**从源码构建时** `web/dist` 里只有一张占位页，打开会告诉你 UI 没打包。要编进去：

```bash
make web       # 按 WEB_VERSION 取 atoll-web 的那个 tag，构建，铺进 web/dist
make build     # bin/atoll 现在带着这版 UI
```

**只改前端时**不必每次重编 —— 起 vite，它把 `/api`、`/ws`、`/obs`、`/files` 代理到节点：

```bash
git clone git@github.com:wanpengxie/atoll-web.git
cd atoll-web && npm install
npm run dev                   # -> http://localhost:5173
```

节点如果监听在别的地址，把开发代理指过去（只影响代理，不会进浏览器 bundle）：

```bash
ATOLL_SERVER_URL=http://127.0.0.1:9000 npm run dev
```

`atoll-web` 还自带一个节点的独立 mock（`npm run mock`），想不起节点先玩玩 UI 可以用。

```
 browser ──► atoll up (:8832)
             ├── web UI      （随二进制发的静态页，同源）
             ├── server      （账本：c0、各频道、注册表）
             └── local device（跑 codex/claude/工具）
```

### 把角色拆成独立进程

`atoll up` 是两个二进制的糖衣，它们共用同样的磁盘 home，所以用 `atoll up` 起的节点以后拆开
不用任何迁移：

```bash
# server —— 持有所有频道的账本；首次运行自装（生成的 root 密码打到日志里）
bin/atoll-server --home ~/.atoll/server --addr 127.0.0.1:8832

# daemon —— 一台计算宿主（"device"）；首次用 device key 绑定（由 `atoll up` 的 provisioning
# 生成，或通过 system.device.create 生成）；之后裸启动，身份保存在 home 里
bin/atoll-daemon --home ~/.atoll/device --server "ws://127.0.0.1:8832/compute" --key <device-key>
```

只有真要把角色放到不同机器上时才需要这样。

## 一个节点由什么组成

`atoll up` 之后，你手里具体有：

- **一个 space，一个 server。** 真相以 SQLite 文件形式在 `~/.atoll/server/`——每个频道一本账
  加一个注册表。没有只存在内存里的东西。
- **一棵以 `c0` 为根的频道树。** 频道用点分名字寻址：`c0`、`c0.lobby`、`c0.dev`、`c0.<用户>`
  （每个注册用户的 home）。`c0` 是管理 space 的地方——频道、agent/工具模板、device、账号，
  全部是在 `c0` 里对 `system` 说话造出来的。`c0.lobby` 是唯一一个在信任域之外的房间：它只为
  让访客注册和登录而存在。
- **三段式 id 的 actor** `<kind>:<seed>:<ts>`——`human:root:…`、`agent:reviewer:…`、
  `tool:my-mcp:…`、`system:registrar:…`、`peer:c0.dev:…`。kind 就是命名空间
  （`tool / human / agent / peer / system`）；seed 是模板 id（人则是 principal）；寻址时可以
  用任意一段不歧义的连续段（`reviewer`、`agent:reviewer`）。单独一个词 `system` 指这个频道的门。
  频道名册在 `/obs/channel/<id>/actors`。
- **每个频道里都有：** 一扇门（`system`），管成员、并把 space 级的词转到 `c0`；一个服务
  actor，代表这个频道回答别的频道（`peer:*`）；以及你坐进去的成员。
- **`c0` 里另外还有：** `registrar`（注册表唯一的写者）、root 的 human cell，以及
  **steward**——你选的那个 codex 或 claude CLI，作为 agent actor 跑在你的设备上、用你自己的
  CLI 登录。
- **device。** device 是一台跑 actor 的机器。`atoll up` 自动挂上本地那台；`atoll-daemon` 再挂
  更多。agent *跑在 device 上*、在那里有工作目录——它的 shell、git、文件都在那儿。
- **一个账号** `root@atoll.local`（root 的 home 就是 `c0` 本身）和一个 bearer token
  （`~/.atoll/server/atoll-token`），同机脚本不用登录。

## 使用节点

### 从 web UI

`atoll-web`（第 3 步）是节点公开契约上的一个客户端——登录、`/ws` 帧、`/obs` 读——仅此而已。
左边是频道，中间是该频道的时间线（人、agent 的回合、系统事件），右边是名册；编辑器对 agent
发 `agent.ask`、对人发 `human.message`。它显示的一切都是从账上读的，它做的一切都是账上的一条消息。

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

// 2. 让 c0 的门列出成员
{"v":2,"frame_type":"submit","ref":"r1","payload":{
  "channel_id":"c0","msg_type":"system.member.list","kind":"request",
  "visibility":"public","audience":["system"],"payload":{}}}

// 3. 问 steward 一句（audience 允许省略 id 的段："steward" 会解到名册里的 agent:steward:<ts>）
{"v":2,"frame_type":"submit","ref":"r2","payload":{
  "channel_id":"c0","msg_type":"agent.ask","kind":"request",
  "visibility":"public","audience":["steward"],
  "payload":{"text":"reply PONG"}}}
```

submit 帧里写裸参数；账本把它包成请求唯一的那种形 `{"body": <参数>}`，你在 feed 上看到的就是包好的。
回执带 `payload.message_id`；答复随后以
`feed` 帧到达，`kind:"response"`、`parent_id` 等于那个 id、终态 `status` 为 `completed` 或
`failed`。请求未关闭时，agent 以指向同一 request 的 provisional response 报告 turn、stage、
tool 过程（`payload.status:"processing"`，细节位于 `payload.process`），最后再发送唯一终态响应。

**管理是一套词表，不是一个管理面板。** 全部是对 `system`（门）的请求；space 级的词在 `c0`
里说，频道级的词在那个频道里说：

| 族 | 词 | 在哪说 |
|---|---|---|
| 频道 | `system.channel.create/get/list/set/delete`、`system.channel.template.*` | `c0` |
| agent 与工具的 class | `system.actor.template.create/get/list/set/delete`、`system.actor.overlay.set/delete` | `c0` |
| 机器 | `system.device.create/list/attach/detach/delete` | `c0` |
| 账号 | `system.principal.create/get/list/delete` | `c0` |
| 成员 | `system.member.create/get/list/delete/restart/admit` | 频道本身 |
| 凭证 | `system.credential.set` | 频道本身 |
| 日志 | `system.log.recent` | 频道本身 |

agent 应答 `agent.ask / steer / interrupt / queue / stop / compact / select / context / fork`；
人应答 `human.message / ask / approve`；每个 actor 都应答 `actor.describe`（它的 manifest：
class、能力、词）。完整形状就是 [`protocol/message/system.go`](protocol/message/system.go) 和
[`platform/lagoon/contracts.go`](platform/lagoon/contracts.go) 里的 Go struct。

### 常见操作

**再坐进一个 agent。** agent 是*模板*（一个 class——`codex`、`claude`、`script`——加配置），
然后*坐进*某个频道；一个模板可以有多个实例，除非声明为 `singleton`：

```jsonc
// 在 c0 里，发给 system —— 声明模板
// id 是键；name 是坐进来的那个成员的名字（小写 a-z、0-9、'-'），它会成为该成员 actor id 的中间段
{"msg_type":"system.actor.template.create","payload":{
  "id":"reviewer","name":"reviewer","description":"负责评审。",
  "class":"claude","visibility":"private",
  "singleton":false,"config":{/* 各 class 自己的配置 */}}}

// 在目标频道里，发给 system —— 坐进一个实例，成员是 agent:reviewer:<ts>
{"msg_type":"system.member.create","payload":{"decl_id":"reviewer"}}
```

**挂一个 MCP server——两条消息，不用重编。**

```jsonc
// 在 c0 里，发给 system
{"id":"my-mcp","name":"my-mcp","description":"我的 MCP server。",
 "class":"mcp","visibility":"private","singleton":false,
 "config":{"name":"testsrv","transport":"http","url":"http://127.0.0.1:8931/mcp"}}

// 在频道里，发给 system   -> {"status":"completed","member":"tool:my-mcp:<ts>"}
{"decl_id":"my-mcp"}
```

这个 actor 出生时就连上 server 并发现它的 `tools/list`；每个工具变成一个以你声明的 `name`
为前缀的消息类型（`testsrv.echo`、`testsrv.add`……），对它 `actor.describe` 能列出。本地子进程
用 `transport:"stdio"` 加 `command` / `args` / `cwd` 代替 `url`。之后频道里的任何人——脚本、
人、别的 actor——都可以对它 `submit` 一条 `testsrv.echo`。

**建一个频道并给它一个 agent。** 在 `c0` 里 `system.channel.create {name, recipe}`；recipe
列出要坐进去的模板。新频道按冻结的配方一步刻出来，并以 `peer:<qualified name>:<ts>` 出现在
`c0` 的名册里——像任何成员一样可以直接对它说话。

**把另一台机器接成 device。** 在 `c0` 里用 `system.device.create` 要一把 key，然后在另一台机器上：

```bash
bin/atoll-daemon --home ~/.atoll/device --server "ws://<node>:8832/compute" --key <device-key>
```

它会出现在 `/obs/space/daemons` 里，并像本地那台一样能承载 agent 和工具。

## 设计要点

下面这些是结构性的性质（由代码形状强制、`archtest/` 检查），不是约定。推理见
[docs/architecture](docs/architecture/README.md)。

- **账本即真相。** 先追加、后可见；没有旁路，没有 REST "send"；可重放、可恢复、可 fork。
- **身份比在任者长寿。** 模型进程重启、升级、换代，还是同一个成员。provider 把用量/步骤写成事件、
  遵守停止协议即可接入。
- **权限在膜上，不在 prompt 里。** 门在频道边界上统一判；没有 token 冒充。
- **缺席恒不销毁。** 一条收敛循环；没有级联 kill，没有隐藏定时器。
- **组织层 = 一份协议 + 一个服务成员。** job、审批、额度、改道靠往频道里加一个成员安装
  （Slack "add app" 同形）；操作系统零改动。已设计，未交付。
- **人是成员。** 审批是发给某个人的一条消息；web UI 只是同一契约的一个客户端。
- **同法自管理。** `c0` 经同一扇门管理所有频道；下一版先在 fork 出的 Atoll 里造好再切换。

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

## 仓库布局

```
cmd/         二进制：atoll（节点）、server、daemon；+ devtools 与共享 internals
scripts/     install.sh（交互式安装器）与仓库 lint 脚本
drivers/     外部世界：tools/（echo、device、kimi、mcp、xhs）、
             agents/（引擎 provider：codex、claude、script）、
             gateway/（人的入口；portal/ = 身份门 + /ws + /obs + /compute）、
             devicehost/（`atoll up` 连上的本地 device）
protocol/    wire 词汇表（envelope、actor id、channel peer 帧、access、resource、system 词表）
runtime/     核心：准入流水线、actor cell/port、sqlite 账本、actor store、
             schedule/timers、access door
lib/         给 actor 作者的标准库：actorbase（每个 actor 都站在上面的 Proc + 动词表基座）、
             behavior、metatool（工具目录）、introspect
platform/    装配：channelhost/、peeractor/ + svcactor/（跨频道路径）、home/（server 侧
             频道 home）、daemonhost/ + compute/（device 侧）、subjectgate/、
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

- [docs/architecture/](docs/architecture/README.md) —— 为什么 agent loop 不是 agent OS、
  五原语、频道作为自治世界、actor、调度、联邦、路线图，以及一个 actor hello-world。
- [docs/dev/](docs/dev/README.md) —— 开发者演练：substrate、actorbase、组合，以及当期施工背后的设计笔记。
- [docs/production/](docs/production/README.md) —— 定位、竞品笔记、采用策略。
- [docs/credential-system-walkthrough.md](docs/credential-system-walkthrough.md) —— 凭证如何在访问面里流动。
- [`atoll-web`](https://github.com/wanpengxie/atoll-web) —— 浏览器客户端及其契约笔记（含一个节点的独立 mock）。

## 状态

**v0.01，pre-release。** 核心（身份、频道、账本、成员身份、device、定时器）已实现并强制，项目
正在用自己开发自己；下一步是组织层。1.0 之前：

- **不对数据负责**——没有升级或迁移路径；schema、home、wire 方言都会变；新二进制可能拒绝或重刻
  旧的 `~/.atoll`（安装器会提议把它挪开）。
- **没有 deprecation 周期**——消息类型、帧、Go 包表面都在动；web 客户端和节点必须一起动
  （对不上表现为 `type_unsupported` 或被拒的帧）。
- **边界**——一个节点一个信任域；`atoll up` 只管本地 device，其它手动挂。缺口见
  [今天能做什么](#今天能做什么2026-08)。

## 许可证

[Apache-2.0](LICENSE)。Atoll 名称及任何托管服务与代码许可证是两回事。

## 名字

项目叫 **atoll**（小写）；Go module 是 `github.com/wanpengxie/atoll`。atoll（环礁）是无数微小
生物一层一层沉积而成的珊瑚环——没有谁拥有这片礁，它靠沉积生长。这就是设计。
