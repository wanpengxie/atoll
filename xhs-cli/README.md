# coagent-xhs

coagent 的 xhs 业务命令行（M1.1-T1）。

5 个子命令：

```
coagent-xhs publish --title <T> --content <md path> [--images a,b] [--tags x,y]
coagent-xhs search <keyword> [--limit N]
coagent-xhs get-my-recent [--limit N]
coagent-xhs get-note --note-id <ID>
coagent-xhs publish-status --note-id <ID>
```

## Build

```
go build ./cmd/coagent-xhs       # binary 输出到当前目录
```

## Provider 切换

环境变量 `COAGENT_XHS_BACKEND`：

- `mock`（默认）：返回固定 fixture，publish 用 ulid 生成 note_id；不依赖外部服务
- `real`：把命令翻译成 daemon HTTP RPC `device.command.send`，立即返回 `{correlation_id, status:"dispatched"}`，不阻塞等真实结果

`real` 模式必填 env：

```
COAGENT_DAEMON_HTTP   # e.g. http://127.0.0.1:7070
COAGENT_DAEMON_TOKEN  # daemon Bearer token
COAGENT_CHANNEL_ID    # dispatch 落地 channel id
```

## 输出 envelope

stdout 一律单行 JSON：

- 成功：`{"ok":true,"data":{...}}`
- 失败：`{"ok":false,"error":{"code":"...","message":"..."}}`

退出码：`0`=ok，`1`=运行/网络/业务错误，`3`=参数错误。

## Test

```
go test ./...
```

`internal/xhs` 含 `mock_provider_test.go` 与 `real_provider_test.go`（用 `httptest` 起 fake daemon，无需联调真 daemon）。

## 范围

T1 仅落 binary 自身。daemon 端 `device.command.send` RPC 处理与 extension 端 cmd handler 由 T3/T2 实现，本仓库不涉及。
