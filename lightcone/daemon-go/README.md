# daemon-go

Go re-implementation of the lightcone daemon (replaces `lightcone/daemon/`,
the Node daemon, after M1.3-T16 cutover). During M1.3 development the two
stacks coexist: the Node daemon keeps serving traffic; every new line of
code lands here.

Tracking spec: [.dalek/pm/m1.3-tickets.md](../../.dalek/pm/m1.3-tickets.md).

## Layout

```
cmd/
  daemon/main.go      ; daemon entrypoint (placeholder until T1+)
  worker/main.go      ; worker entrypoint (placeholder; runs kimismoke)
internal/
  store/              ; T1  channel-local + daemon-level SQLite layer
  bootstrap/          ; T3  channel-create 9-step saga + reconcile
  registry/           ; T4+T5 actor_registry + type_registry + validators
  harness/            ; T7  daemon-side HTTP binding
  trigger/            ; T8  trigger gateway
  scheduler/          ; T9  scheduled wake-ups
  supervisor/         ; T6  worker_locks CAS + supervisor loop
  ledger/             ; T6  action_ledger reserve/commit
  adapter/            ; T13+T14 adapter framework + xhs rewrite
  worker/             ; T10+T11 worker runtime + tool actor wrappers
pkg/
  canonical/          ; T2  RFC 8785 canonical JSON + SHA-256
  v4types/            ; T2  Envelope / kind / reason enums
  harness/            ; T7  shared 9-step body (in-worker bus binding)
  kimismoke/          ; T0  go-kimi integration smoke (echo provider)
```

Most `internal/*` and `pkg/*` directories ship as `doc.go` skeletons in T0
and grow real implementations through T1–T16.

## Build

```bash
cd lightcone/daemon-go
go build ./...
go test ./...
```

Or via the root Makefile:

```bash
make daemon-go-build
make daemon-go-test
make daemon-go-lint
```

## go-kimi smoke

`pkg/kimismoke` adapts go-kimi's `examples/01_basic_turn` to use the
in-process `echo` provider so the smoke runs in CI without an
`OPENAI_API_KEY` or any network access. It exercises the four SDK calls
the M1.3 worker runtime (T10) will depend on: `NewAgent`, `Run`,
`LastResult`, `Close`.

`cmd/worker/main.go` calls into it during T0; T10 replaces that with the
real v4 ABI adapter loop.

## CI

The GitHub Actions workflow [.github/workflows/go-ci.yml](../../.github/workflows/go-ci.yml)
runs `go build`, `go test`, and `golangci-lint` (config in
[.golangci.yml](../../.golangci.yml)) for the `lightcone/daemon-go`
module on every push / PR.
