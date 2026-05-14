# Coagent

M1.5 单栈版本：4 个 Go binary（server / daemon / worker / cli）+ 原生 Vite UI +
xhs Chrome 扩展（device 通道）。旧 Node 双栈（lightcone/）已在 T9 全归档到
`archive/`，参考 `archive/<dir>/ARCHIVE.md`。

## Quickstart

```bash
# 1. 装依赖（Go modules + pnpm workspace + lint / migrate 工具）
make install

# 2. 构建（Go 4 binary + ui dist + chrome 扩展）
make build
ls bin/   # → coagent-server  coagent-daemon  coagent-worker  coagent-cli

# 3. 建 server 端 sqlite schema（demo 自用，无数据迁移；要重置直接
#    rm data/server.db 再 make migrate）
make migrate

# 4. 起服务（前台 / 各开一个终端）
./bin/coagent-server                                          # 监听 :8080（默认）
./bin/coagent-daemon --server-url ws://localhost:8080 --key dev

# 5. 起 UI（开发模式）
pnpm --filter ui dev                                          # http://localhost:5173

# 6. 烟雾测试（手工）
#    - 浏览器打开 UI，注册用户 / 建 workspace / 建 channel
#    - 在 server 后台为 channel 创建 xhs device，复制 api-key
#    - 加载 adapters/device/xhs/extension/app/chrome-extension/.output/chrome-mv3/
#      到 chrome（开发者模式），popup 填 server URL + api-key 点连接
#    - 在 UI 内发送 channel message，观察 worker 调度
#    - feishu outbound：channel 配置 webhook 后由 worker 发起 outbound
```

## 顶层 make targets

```bash
make install     # 装 Go modules + pnpm + golangci-lint + go-arch-lint + golang-migrate
make build       # build-go + build-ui + build-ext（4 binary + ui dist + 扩展）
make build-go    # 仅编 Go binary（cmd/{server,daemon,worker,cli}）
make build-ui    # 仅编 ui/（vite build）
make build-ext   # 仅编 chrome 扩展（adapters/device/xhs/extension）
make test        # go test ./... + pnpm -r --if-present test
make lint        # 5 类 lint：golangci-lint / go-arch-lint / banned-words / kernel-protocol / docs
make migrate     # 给 server sqlite 跑 schema migration（server/store/migrations/*）
make dev         # 起 server + daemon + ui dev（best-effort）
make clean       # 删 bin/ dist/ ui/dist
```

## 二进制职责（M1.5）

| Binary | 作用 |
|--------|------|
| `bin/coagent-server` | 多 channel 协调中心：用户 / channel / device / push / placement / daemonbus / devicebus |
| `bin/coagent-daemon` | 单机进程编排：channel actor host、xhs device 反连、feishu outbound、worker fork/supervise |
| `bin/coagent-worker` | Agent runner 子进程；由 daemon fork（v4 envelope IPC） |
| `bin/coagent-cli`    | 业务 + 运维 CLI：channel / message / xhs publish / admin |

## 目录结构（v5 单栈）

```
kernel/             # 协议合同 + 纯类型（v4 envelope / actor / channel / placement / topology / addressing）
runtime/            # daemon 侧：bootstrap / scheduler / supervisor / transit / ipc / workerhost / store
adapters/           # device + messaging adapter（device/xhs/extension/、messaging/feishu/ 等）
server/             # server 进程内部模块（gateway / daemonbus / devicebus / placements / catalog / identity / pushhub）
cmd/{server,daemon,worker,cli}/  # 4 binary 入口（main.go）
pkg/v4types/        # 跨 binary 共享的 v4 互操作类型（v0~v3 grandfather 兼容）
ui/                 # 原生 Vite UI（替换 lightcone/public/ demo）
archive/            # 旧 Node 双栈归档（agent-binary / cli-node / lightcone-* / ops-node-scripts / 等）
.dalek/pm/          # 工程权威 spec（v4-* / m1.5-* / m1.4-* / m1.3-*）
```

## 路径与持久化

Daemon 本地运行时数据在 `~/.coagent/{COAGENT_PROJECT_KEY}/`：

- `daemon.sock`：daemon 本地 socket（除非 `COAGENT_DAEMON_SOCKET` 覆盖）
- `channels/`：active channel workdir
- `archived/`：archived channel workdir
- `machine.key`：daemon 注册到 server 后写入的 machine api key（chmod 600）

Server 本地 sqlite：`./data/server.db`（demo 自用，无数据迁移；销毁重建用
`rm data/server.db && make migrate`）。

项目级临时 scratch：`.coagent-local/`（git ignore）。

工程规划文档：`.dalek/pm/` 顶层 spec 入仓；`.dalek/runtime/` + worker-local
状态由 dalek 管理且 ignore。

## 协议合同

- v4 envelope / kind / reason / actor 类型：`kernel/message/`（Go）
- 6 大 ownership invariants：`.go-arch-lint.yaml` 顶层配置 + `make lint-arch`
- 禁用词约束（mcp / dogfood / 1studio / lightcone）：`scripts/lint-banned-words.sh`
- 文档交叉引用校验：`scripts/lint-docs.sh`

权威 spec：

- v4 protocol：`.dalek/pm/v4-{layer0,layer1,layer2,layer3,layer4}-spec.md` +
  `v4-message-definition.md`
- m1.5 工程纪律：`.dalek/pm/m1.5-server-rewrite-and-restructure.md` +
  `m1.5-tickets.md`

## 编排（demo 期最简）

demo 自用阶段不上 pm2 / systemd，直接前台/后台 `bin/coagent-*` 即可。生产化
编排（systemd unit、日志轮转、健康检查 endpoint）M2.0 再补。

## Troubleshooting

- server 启动失败：检查 `./data/server.db` 是否存在；不存在则 `make migrate`
- daemon 连不上 server：检查 `--server-url` 和端口；本地 `ws://localhost:8080`
- ui 看不到 channel：检查 server 是否在监听、登录态是否丢失
- chrome 扩展报 "Server 不可达"：popup 内 Server URL 改成真实 server 部署域名
  （默认 `https://coagent-server` 是 placeholder）
- 烟雾测试需要：本机 chrome + 已登录的 xhs 账号 + feishu webhook（如果走
  feishu outbound）

## 历史归档

`archive/` 下保存 M1.0~M1.2 的 Node 双栈实现，仅做参考、不参与构建。每个
子目录有 `ARCHIVE.md` 写明归档原因 / 取代物 / 安全删除时间窗：

- `archive/agent-binary/`            — 旧 Node agent runner
- `archive/cli-node/`                — 旧 Node CLI
- `archive/lightcone-node-server/`   — 旧 Node lightcone server
- `archive/lightcone-node-daemon/`   — 旧 Node lightcone daemon
- `archive/lightcone-daemon-go/`     — M1.3 过渡 Go daemon 模块
- `archive/lightcone-feishu-bridge/` — 旧 Node feishu bridge
- `archive/lightcone-demo-design/`   — M1.0~M1.2 demo 设计稿
- `archive/lightcone-docs/`          — 旧 lightcone 子项目文档
- `archive/lightcone-misc/`          — lightcone/ 顶层杂项 + 历史 .dalek/pm 笔记
- `archive/packages-payload-types/`  — 旧 TS 共享 payload 类型包
- `archive/workspace-template/`      — 旧 channel workspace 文件骨架
- `archive/ops-node-scripts/`        — 旧 ops Node 脚本（doctor / register-machine / 等）

M2.0 启动前可按需进一步清理。
