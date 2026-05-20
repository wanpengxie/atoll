# coagent-xhs

coagent 的 xhs 业务命令行（M1.3-T14 v4 重写；M1.5-T5 从 `xhs-cli/`
重组到 `adapters/device/xhs/cli/`，并入根 go module）。

5 个子命令（按 L4 §2.3 对齐）：

```
coagent-xhs publish     --title <T> --content <md path> [--images a,b] [--tags x,y]
coagent-xhs search      <keyword> [--limit N]
coagent-xhs recent      [--limit N]
coagent-xhs get-note    --note-id <ID> | --url <URL> | --note-id+--xsec-token
coagent-xhs sync-cookie
```

> **M1.3-T14 变更**：legacy `device.command.send` RPC 入口下线；real
> provider 改为 spawn `coagent ask --type xhs.<op> --audience
> tool:xhs-adapter`（L4 §2.3.2 "CLI 是 daemon RPC 的 domain wrapper"）。
>
> - `xhs.get-my-recent`  → `xhs.recent.fetch`（CLI 子命令也从 `get-my-recent` 改名 `recent`）
> - `xhs.get-note`       → `xhs.note.fetch`
> - `xhs.publish-status` → 删除（v4 type 表无对应；agent 通过 message store
>   按 correlation_id 查询 response 替代）
> - `+ xhs.cookie.sync`  → 新增 `sync-cookie` 子命令

## Build

```
go build -o coagent-xhs ./adapters/device/xhs/cli   # binary 输出到当前目录
```

`real` 模式需要 `coagent` binary 在 PATH，或通过 `COAGENT_BIN` env
显式指定路径。`coagent` 主入口在 M1.5 由 cmd/cli (T7) 提供；T7
完成前可继续使用 archived daemon-go cmd/coagent 构建产物作 stub。

## Provider 切换

环境变量 `COAGENT_XHS_BACKEND`：

- `mock`（默认）：返回固定 fixture，publish 用 ulid 生成 note_id；不依赖外部服务
- `real`：把命令翻译成 v4 message envelope，spawn `coagent ask` 子进程发到 daemon；
  立即返回 `{correlation_id, id, status:"dispatched", dedupe?}`，不阻塞等真实结果

`real` 模式必填 env（与 `coagent` CLI 共享命名；legacy 名称兼容）：

```
DAEMON_URL              # daemon HTTP base URL (e.g. http://127.0.0.1:7070)
COAGENT_AUTH_TOKEN      # daemon Bearer token
COAGENT_CHANNEL_ID      # dispatch 落地 channel id
COAGENT_BIN             # 可选：coagent binary 路径，默认从 PATH 解析
```

兼容的 legacy 名称（自动回退）：

```
COAGENT_DAEMON_HTTP  → DAEMON_URL
COAGENT_DAEMON_TOKEN → COAGENT_AUTH_TOKEN
```

## 输出 envelope

stdout 一律单行 JSON：

- 成功：`{"ok":true,"data":{...}}`
- 失败：`{"ok":false,"error":{"code":"...","message":"..."}}`

退出码：`0`=ok，`1`=运行/网络/业务错误，`3`=参数错误。

`real` 模式错误 code 透传 daemon harness reason（如 `harness_request_audience_invalid`、
`actor_not_registered`），或 wrapper code：

- `coagent_unavailable` — spawn coagent 失败（binary 缺失 / 无权限）
- `coagent_usage_error` — coagent 解析 argv 失败（exit 2）
- `coagent_reject` — coagent 报告 reject 但 stderr 解析失败
- `coagent_infra` — coagent 报告基础设施错（exit 4）
- `coagent_no_binding` — coagent 找不到 binding（exit 5）
- `coagent_flag_format` — coagent flag 格式错（exit 6）
- `coagent_failed` — 其它非零 exit
- `invalid_daemon_response` — coagent stdout JSON 缺字段

## Test

```
go test ./...
```

`internal/xhs` 含 `mock_provider_test.go` 与 `real_provider_test.go`：
real provider 通过注入 `CoagentRunner` stub 验证 argv / payload / env 拼装，
无需联调真 `coagent` binary。

## 范围

M1.3-T14 落 binary 本身的 v4 改造；daemon-go 端 `xhs` adapter（
`daemon-go/internal/adapters/xhs`）与 extension callback HTTP handler 由同
ticket 同步交付。
