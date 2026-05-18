# tests/e2e — end-to-end smoke suite

## Purpose

Catch wiring / assembly bugs that single-binary unit tests miss. Every
case in this directory exists because the owner hit a class of bug
that was invisible to `go test ./...` but blew up the moment the
server, daemon, and worker were actually wired together.

The suite is gated behind the `e2e` build tag so the default
`go test ./...` stays fast for the inner dev loop.

## Running

```sh
make build-go         # produces bin/coagent-{server,daemon,worker}
make e2e-smoke        # runs the 7 cases against subprocesses on random ports
```

Set `E2E_VERBOSE=1` to surface server/daemon stdout+stderr unconditionally
(the suite already dumps them on test failure).

## Coverage (M1 first pass)

| Test                                              | Catches                                                 |
| ------------------------------------------------- | ------------------------------------------------------- |
| `TestE2E_PostMessage_HappyPath`                   | write deadline, ack frame_id pairing, SendAndAwait      |
| `TestE2E_AgentReply_SingleFinalEnvelope`          | worker spawner unwired, LLM chunk spam regression       |
| `TestE2E_GetMessages_ShapeContract`               | GET /messages JSON shape drift (nested Envelope wrapper) |
| `TestE2E_WSPushReceived`                          | push fan-out envelope contract drift                    |
| `TestE2E_DaemonbusKeepalive_PingPong`             | WS half-open / missing ping/pong / write deadline       |
| `TestE2E_DaemonRestart_Reconnect`                 | placement reclaim / reconnect path regressions          |
| `TestE2E_ListMessages_AfterAgentReply_NotEmptyPayload` | empty payload write-path regression                  |

## Mock provider

Each test stack runs the daemon with `--worker-provider=mock`. The
mock bridge is configured via env vars set by the harness:

* `COAGENT_MOCK_SINGLE_SHOT=1` — every trigger emits exactly ONE
  `agent.text` envelope whose payload already carries
  `next_action=done`. Skips the otherwise-emitted terminal frame.
* `COAGENT_MOCK_REPLY_TEXT=pong` — overrides the reply text so
  assertions can pin to a known sentinel.

See `runtime/worker/mock_bridge.go` (`EnvKeyMockSingleShot`,
`EnvKeyMockReplyText`) for the source of truth.

## Adding a new test

1. Drop a new `*_test.go` under `tests/e2e/` with `//go:build e2e`
   as the first line.
2. Call `harness.Start(t, harness.Options{})` to spin up an isolated
   stack. `t.Cleanup` already handles shutdown.
3. Use the helper methods on `*harness.Stack` for HTTP/WS/sqlite
   interactions; reach into the per-channel sqlite via
   `s.ListChannelMessages(chID)` for assertion-grade reads.
4. Tests run in parallel-safe isolation by default (each gets its own
   tmp dir + random port). If a test takes longer than ~15s it should
   poll with `harness.Eventually(...)` rather than `time.Sleep`.

## Out of scope (second pass candidates)

* device session bind / xhs adapter publish path (needs an extension
  WS client mock; non-trivial to fake without a Chromium-like
  upgrader)
* multi-daemon reclaim races
* view-sync gap drain replay
* M1.6 trigger gateway dedupe under contention
