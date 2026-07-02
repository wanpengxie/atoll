# Coagent

多 actor 协作平台：actor 通过 envelope 协议在 channel 内互相发消息，server 持有 truth（per-channel sqlite），daemon 提供算力（host actor cell）。

## 架构

```
protocol/    协议类型（envelope, actor, channel）
runtime/     基座运行时（harness 9步管线, actorrt cell/port, sqlite store）
lib/         stdlib（behavior, callkit, metatool, channelkit, sysactor, introspect）
platform/    channel 运行平台（server 侧 ChannelHome + daemon 侧 RunCompute）
app/         产品 HTTP API（gin, identity, workspace, channel, daemon, WS）
actors/      actor 实现（echo, feishu, agent/kimi, xhs）
cmd/         二进制入口（server, daemon, cli）
sdk/         Go SDK
web/         前端（React Vite UI + xhs Chrome 扩展）
```

## Quickstart

```bash
# 1. 构建
go build -o bin/atoll-server ./cmd/server
go build -o bin/atoll-daemon ./cmd/daemon
go build -o bin/atoll-cli    ./cmd/cli

# 2. 起 server
bin/atoll-server --db /tmp/atoll-dev/app.db --channel-db-dir /tmp/atoll-dev/channels

# 3. 起 daemon（echo actor，不需要外部凭证）
#    先在 UI 或 CLI 创建 daemon 拿到 api-key，绑定到 channel
bin/atoll-daemon --server ws://localhost:8080/compute?key=<api-key>&channel=<chID> \
                   --key <api-key> --actors echo

# 4. UI（开发模式）
cd web/ui && pnpm dev   # http://localhost:5173

# 5. 测试
go test ./...
```

## 二进制

| Binary | 作用 |
|--------|------|
| `atoll-server` | HTTP API + WS 推送 + 所有 channel 的 truth 持有者 |
| `atoll-daemon` | WS 连 server，host actor cell（echo/feishu/...） |
| `atoll-cli`    | 管理 CLI（channel/daemon/message 操作） |

## 核心设计

- **Server IS truth**：所有消息写入只在 server 侧的 per-channel sqlite，daemon 不持有 truth
- **Envelope 协议统一**：所有 actor 交互通过同一种 envelope 格式（kind=request/response/event）
- **Cell/Port 对称**：in-process actor（cell）和 remote actor（port over WS）对 actorrt 来说一样
- **Closure 三作者**：request 的终态由三条路径保证——actor 回复（author#1）、caller 超时（author#2）、actor 死亡（author#3）
- **Harness 9 步管线**：每条消息写入 truth 前经过 9 步校验（caller auth → envelope shape → channel match → sender consistent → kind+audience → response pairing → dedupe → commit）

## 接入新 actor

实现一个 `Receive(ctx context.Context, env *message.Envelope) error`，注册到 daemon registry：

```go
// actors/hermes/hermes.go
func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
    // 处理请求，写回复（pen 焊死本 actor 身份，无需填 Sender/ChannelID）
    _, err := a.pen.Write(ctx, responseEnvelope)
    return err
}
```

```go
// cmd/daemon/main.go
var registry = map[string]func(harness.Pen) actorrt.Actor{
    "echo":   func(p harness.Pen) actorrt.Actor { return echo.NewActor(p) },
    "hermes": func(p harness.Pen) actorrt.Actor { return hermes.NewActor(p) },
}
```

## 项目名 vs module path

- **项目名：atoll**（小写）
- **Go module：`github.com/wanpengxie/atoll`** — import path 与项目名统一为 atoll（曾用名 atoll/coagent，2026-07 系统性更名）
