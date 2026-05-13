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
  migrate/main.go     ; T1  schema bootstrap + Node→v4 migration CLI
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

## v4types + canonical (T2)

`pkg/v4types` exposes the v4 protocol baseline as Go types: the
`Envelope` struct (17 content + 4 delivery-metadata + `is_terminal` +
`seq` columns), the three message ADT kinds (`KindEvent` /
`KindRequest` / `KindResponse`), the 4-value `SenderKind` enum, the
3-value `Visibility` enum, and the three closed reason sets from L1
§10.3 (`HarnessRejectReason`, `InstallReason`, `TerminalFailureReason`)
with `HTTPStatus()` mapping aligned to L2 §3.6.1.

`pkg/canonical` provides the RFC 8785 + SHA-256 hash mandated by
L2 §1.4.10.2:

```go
hash, err := canonical.CanonicalHash(envelope)          // 14-key content hash
phash, err := canonical.CanonicalHashPayload(payload)    // adapter response id
buf, err := canonical.CanonicalizeJSON(rawJSON)          // canonical bytes
```

The hash is hex lowercase (64 chars), deterministic across platforms,
and excludes the store-derived columns (`ts_received`, delivery
metadata, `is_terminal`, `seq`) per L1 §10.2.2. T6 / T7 build on this
for action_ledger keys and harness step 0.5 / step 8 content compare.

## bootstrap saga (T3)

`internal/bootstrap` runs the L2 §1.4.7 channel-create 9-step saga and
its reconcile loop:

```go
saga := bootstrap.New(daemonDB)
res, err := saga.ChannelCreate(ctx, bootstrap.CreateParams{
    CreateRequestID: "<server-uuid>",
    ChannelID:       "<server-assigned>",
    WorkdirPath:     "/path/to/channel/workdir",
    HumanMembers:    []bootstrap.HumanMember{{ActorID: "user-001"}},
    ChannelAgent:    bootstrap.ChannelAgentSpec{ /* default channel-agent */ },
    ToolAdapters:    []bootstrap.ToolAdapterSpec{ /* … */ },
    BusinessTypes:   []bootstrap.TypeRegistryRow{ /* … */ },
})
```

Idempotency is keyed on `create_request_id`: the same id replays
return `(channel_id, completed)` after success;
`ErrBootstrapInProgress` while a saga is still running;
`ErrBootstrapRolledBack` when the previous attempt failed
(caller must switch id).

The 9 steps land in this order (each fail compensates):

1. INSERT `bootstrap_registry` row (status=in_progress).
2. mkdir workdir + `OpenChannel(messages.sqlite)`.
3–7. Inside a single channel-local `BEGIN IMMEDIATE`:
   seed `system` actor → human members → channel agent →
   tool adapters (`actor_registry` + `type_registry` per L2 §3.5
   install order) → business `type_registry` rows.
8a. INSERT `messages` row for `system.event payload.kind=channel_created`
   with deterministic id `bootstrap:<create_request_id>` (INSERT OR
   IGNORE for reconcile-safe replay).
8b. CAS `UPDATE bootstrap_registry SET status='completed'`.

Crash between 8a and 8b → reconcile sees `in_progress`, retries 8a
(no-op dedup via messages.id UNIQUE) + 8b → status=completed.

HTTP surface (auto-mounted by `bootstrap.RegisterRoutes(mux, saga)`):

| Route | Method | Purpose |
|---|---|---|
| `/api/channel/create` | POST | drive ChannelCreate (200 / 409 / 400 / 500) |
| `/api/channel/list`   | GET  | enumerate `status='completed'` rows for server-side cache reconcile |

Tests use `internal/store.OpenDaemon` + `t.TempDir()` workdirs and a
failpoint hook (`withFailpoints`) to assert the rollback compensation
for every step. No real e2e: see `internal/bootstrap/*_test.go`.

## migrate (T1)

`cmd/migrate` is the schema bootstrap + Node-daemon-data import CLI. It
owns three subcommands:

```bash
# Initialize an empty channel sqlite (6 v4 tables + 9 indexes).
go run ./cmd/migrate init <channel.sqlite>

# Initialize the daemon-level sqlite (bootstrap_registry + index).
go run ./cmd/migrate init-daemon <daemon.sqlite>

# Import a legacy Node daemon channel sqlite into v4 form. The source
# is opened read-only; the destination is created if missing.
go run ./cmd/migrate from-node --src <node.sqlite> --dst <channel.sqlite>
```

The transform rules follow `.dalek/pm/m1.3-v4-foundation-spec.md` §4.1
verbatim — see `internal/store/migrate_typemap.go` for the
type-mapping table and `migrate_from_node.go` for the per-column
rewrite (audience, visibility, correlation_id, doc_refs, payload.body
strip, attempts rename, sender_kind coercion, etc.).

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
