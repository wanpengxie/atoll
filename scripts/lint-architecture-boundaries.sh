#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "[lint-architecture-boundaries] $1" >&2
  exit 1
}

if ! command -v rg >/dev/null 2>&1; then
  fail "ripgrep (rg) is required"
fi

if rg -n "^\\s*(HarnessChain|Correlation|ErrorPolicy|DeviceTransit|ActorReadiness)\\s+" kernel/adapter/ctx.go -S; then
  fail "ModuleContext must not expose raw capability fields"
fi

if rg -n "mctx\\.(HarnessChain|Correlation|ErrorPolicy|DeviceTransit|ActorReadiness)" kernel/adapter adapters -S; then
  fail "adapter-facing ModuleContext/raw capability access is forbidden"
fi

if rg -n "github\\.com/wanpengxie/ActOS/adapters/framework" adapters/device -g'*.go' -g'!*_test.go' -S; then
  fail "device adapters must not import adapters/framework implementation; use ModuleContext capabilities"
fi

if rg -n "\\b(FailNow|EmitOrphanCallbackEvents|OrphanCallbackType|OrphanCallbackEvent)\\b" adapters/framework adapters/device -g'*.go' -g'!*_test.go' -S; then
  fail "framework failure/orphan helpers must not be public adapter-callable capabilities"
fi

if rg -n "eventType != orphanCallbackType" adapters/framework/events.go -S; then
  fail "adapter-facing EmitEvent must not allow framework-owned orphan callback event construction"
fi

if ! rg -n "use ReportOrphanCallback" adapters/framework/events.go >/dev/null; then
  fail "adapter-facing EmitEvent must explicitly reject orphan callback type"
fi

if rg -n "COAGENT_CHANNEL_DB|OpenChannelStore|ChannelStore|actor_registry|adapter_state|type_registry|database/sql|modernc\\.org/sqlite" adapters/llm/kimi -S; then
  fail "Kimi worker/adapter must not read raw channel sqlite or registry tables"
fi

if rg -n "COAGENT_CHANNEL_DB|EnvChannelDB|dedupePublish|database/sql|modernc\\.org/sqlite|messages WHERE|channel\\.sqlite" adapters/device/xhs/cli/internal -S; then
  fail "xhs CLI must not read raw channel sqlite/message log for business truth"
fi

if rg -n "actors/.*/status|QueryActorStatus|actorStatusWaiter|handleActorStatus" server ui -S; then
  fail "server/UI actor status endpoints must use reserved actor.status envelope calls"
fi

if [ -e adapters/xhs ]; then
  fail "legacy adapters/xhs scaffold package must not exist"
fi

if rg -n "use-scaffold-xhs|XHSScaffoldFactory|DeviceXHSFactory|DeviceXHSActorSeed" cmd/daemon -g'*.go' -g'!*_test.go' -S; then
  fail "production daemon must not expose legacy xhs scaffold/device-direct paths"
fi

if rg -n "use-scaffold-xhs|UseScaffoldXHS|XHSScaffoldFactory|DeviceXHSFactory|DeviceXHSActorSeed" tests -S; then
  fail "tests must not retain legacy xhs scaffold/device-direct switches"
fi

if rg -n "devicexhs\\.New|DefaultInstallSpec\\(" cmd/daemon -g'*.go' -g'!*_test.go' -S; then
  fail "production daemon must not instantiate or statically seed the xhs device adapter"
fi

if rg -n "github\\.com/wanpengxie/ActOS/adapters/xhs" cmd adapters pkg kernel runtime server tests internal -g'*.go' -S; then
  fail "product code/tests must not import deleted legacy adapters/xhs package"
fi

legacy_xhs_actor="tool:xhs-adapter"
if rg -n "$legacy_xhs_actor" cmd adapters pkg kernel runtime server tests internal ui scripts -g'!*lint-architecture-boundaries.sh' -S; then
  fail "xhs actor id must be canonical tool:xhs"
fi

if rg -n "sqlite3|messages WHERE|type_registry WHERE|COAGENT_CHANNEL_DB" adapters/device/xhs/template.go adapters/llm/kimi/bridge.go -S; then
  fail "agent prompts must not teach raw sqlite/message-log reads"
fi

if rg -n "DefaultMaxPendingMs int64 = 30 \\* 1000" adapters/device/xhs/proto.go -S; then
  fail "xhs adapter timeout must not drift below 300s budget"
fi

if rg -n "Callable:\\s*daemonOnline && routeActive && actorReady|Callable:\\s*daemonOnline && facadeInstalled && actorReady|Callable:\\s*routeActive && facadeInstalled && actorReady" server/devicebus/routes.go -S; then
  fail "device callable projection must include daemonOnline && routeActive && facadeInstalled && actorReady"
fi

if rg -nU "fn, ok := d\\.handlers\\[id\\][\\s\\S]{0,160}if !ok \\{\\s*continue\\s*\\}" runtime/scheduler/deliver.go; then
  fail "scheduler delivery must not treat missing concrete actor handler as success"
fi

if rg -nU "daemon\\.adapter_dispatch_failed[\\s\\S]{0,160}\\}\\s*return nil" cmd/daemon/adapter_wiring.go; then
  fail "adapter dispatch errors must propagate unless framework emitted a terminal"
fi

if rg -n "sent frame', frameLogDetails|Sent successfully → clear from outbox" adapters/device/xhs/extension/app/chrome-extension/entrypoints/background/services/server-devicebus.ts -S; then
  fail "extension callback outbox must delete only after callback ack"
fi

if rg -nU "case c\\.triggerCh <- payload:\\s*c\\.writeTriggerAck" runtime/worker/ipc_client.go; then
  fail "worker IPC dispatch must not ack after in-memory trigger enqueue"
fi

if ! rg -n "func \\(c \\*IPCClient\\) AckTrigger" runtime/worker/ipc_client.go >/dev/null; then
  fail "worker IPC must expose explicit bridge-owned AckTrigger"
fi

if ! rg -nU "caller\\.Deliver\\(&env\\)[\\s\\S]{0,400}ackTrigger\\(ctx, ipc, payload, true" adapters/llm/kimi/channel_tool.go >/dev/null; then
  fail "Kimi intercepted tool responses must explicitly ack their trigger"
fi

if rg -nU "adapterName := deviceAdapterByActor\\[frame\\.AdapterActorID\\][\\s\\S]{0,120}if adapterName == \"\" \\{\\s*return nil\\s*\\}" cmd/daemon/adapter_wiring.go; then
  fail "device callback with no adapter route must return retryable ack error, not accepted nil"
fi

if rg -nU "No TurnEnd seen[\\s\\S]{0,120}return nil" adapters/llm/kimi/bridge.go; then
  fail "Kimi bridge must not ACK a trigger when agent finishes without TurnEnd"
fi

if rg -n "if \\(corr && entryCorr === corr\\) return false" adapters/device/xhs/extension/app/chrome-extension/entrypoints/background/services/server-devicebus.ts -S; then
  fail "extension outbox must use request_id as primary key before correlation_id fallback"
fi

if rg -nU "OnExternalCallbackFrame[\\s\\S]{0,260}parseExternalCallback\\(ctx, frame\\.Payload\\)[\\s\\S]{0,80}return err" adapters/device/xhs/module.go; then
  fail "xhs malformed callback frame must complete or typed-ack, not return generic retryable error"
fi

echo "✅ architecture boundary lint passed"
