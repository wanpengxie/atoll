# Atoll

English | [简体中文](README.zh-CN.md)

**An operating system for agents.** Install it on a machine; agents (codex, claude,
your own), tools (MCP servers, devices) and people then run on it side by side in
shared channels, each with an identity, permissions, a durable ledger of what
happened, and a way to be woken later. Linux gives programs processes, files and
permissions; Atoll gives agents **actors**, **channels** and **membership**.

| Unix | Atoll | |
|---|---|---|
| process | **actor** | a person, an agent, a tool, the system — one kind of identity |
| file system + pipes | **channel** | where work happens; also the ledger — everything is a message in its ordered log |
| byte stream | **message** | request / event / response; the OS does not read the content |
| file | **resource** | files, KV, external objects; control plane here, data plane delegated |
| permission bits | **access** | channel membership decides who may do what |
| cron | **timer** | a durable wake-up; survives restarts |

> **v0.01 — pre-release, not data-safe.** Storage formats, APIs and the wire
> protocol change without deprecation or migration; a newer build may refuse or
> overwrite an older node home. Keep nothing in it you cannot recreate.

## Features

- **One command to a running node** — `atoll up` installs `c0`, the root account,
  a local compute device, and seats a coding agent (codex or claude) as steward.
- **Every message on the ledger** — one append-only SQLite log per channel; the
  server is the only writer; sender identity is stamped by the runtime, not filled
  in by the sender. Audit, replay and recovery come from the log.
- **Channels as the unit of everything** — trust boundary, context, files and
  lifecycle; a tree addressed by name (`c0`, `c0.dev`, `c0.alice`).
- **Membership is the permission** — no per-object ACLs; an agent's act is its
  principal's act.
- **Agents are members, not sessions** — stable identity across restarts and model
  swaps; `codex`, `claude`, `script` engines; many instances per template.
- **Tools mount at runtime** — an MCP server becomes a member with two messages,
  its tools become message types.
- **Machines join as devices** — `atoll-daemon` on another box hosts agents and
  tools with a real shell, files and git.
- **Declarative convergence** — desired state on the ledger vs. host testimony;
  crashes and restarts converge back, nothing is killed by absence.
- **Three surfaces, no back door** — `/ws` frames to write, `/obs` to read,
  `/files` for data; the web UI and scripts use exactly the same ones.
- **Layering machine-checked** — `archtest/` fails the build when a layer is
  crossed; it applies to agent-written code too.

## What works today (2026-08)

| | |
|---|---|
| Install, run, log in, talk to the steward, web UI, scripts, MCP mount, extra devices, multiple channels / agents / accounts | ✅ |
| Agents calling Atoll's own tools from inside their model loop (`call_actor` …) | 🚧 tool port exists; codex / claude workers do not expose it yet |
| Programmatic prompt injection for agents (who am I, where, who is here) | 🚧 designed |
| Jobs, approvals, quotas, message interposition (the organisation layer) | 📐 design only |
| Cross-node federation, DID identity | 📐 direction only |

---

- [Quickstart](#quickstart)
- [How a node is put together](#how-a-node-is-put-together)
- [Using the node](#using-the-node) · [web UI](#from-the-web-ui) · [script](#from-a-script) · [recipes](#recipes)
- [Design notes](#design-notes)
- [Extending: write your own actor](#extending-write-your-own-actor)
- [Repository layout](#repository-layout) · [Development](#development) · [Documentation](#documentation)
- [Status](#status) · [License](#license)

---

## Quickstart

You end up with a node at `http://127.0.0.1:8832`, a `root` account, a coding agent
seated in the root channel `c0` as its **steward**, and a web UI you just open.

### Install a release (the short way)

No Go, no build, no separate front end. This picks the build for your OS and
architecture (the UI is inside it), verifies its sha256, and drops you into the
very same wizard a source install runs:

```bash
curl -f#SL https://raw.githubusercontent.com/wanpengxie/atoll/main/scripts/install.sh | bash
```

Then open `http://127.0.0.1:8832`.

```bash
ATOLL_VERSION=v0.01 ...               # pin a release instead of taking the latest
ATOLL_INSTALL_DIR=/usr/local/bin ...   # where the binary goes (default ~/.local/bin)
```

If the command sits silent for more than a few seconds, that is the network to
GitHub, not the installer: every stage prints progress once the script is
running, so a silent terminal means the script itself is still downloading.

Archives and `checksums.txt` are on [Releases](https://github.com/wanpengxie/atoll/releases);
mac ships as Apple Silicon and Intel builds, Linux as amd64 and arm64, and the
script picks by `uname`.

The codex / claude CLI a steward runs on is still yours to install and log in —
that is your account, not something an installer should claim for you.

What follows is the **from source** route.

**Prerequisites**

| For | Need |
|---|---|
| the node | Go 1.25+ ([go.mod](go.mod)), `make`, `curl` |
| a steward agent | [`codex`](https://github.com/openai/codex) and/or [`claude`](https://github.com/anthropics/claude-code) CLI installed **and logged in** (the installer detects both and lets you pick; you can add one later) |
| the web UI | Node.js 22+ (`make web` builds the [`atoll-web`](https://github.com/wanpengxie/atoll-web) tag named in [WEB_VERSION](WEB_VERSION) into the binary); the node runs without it, the UI is then a placeholder page |

### 1. Build

```bash
git clone git@github.com:wanpengxie/atoll.git
cd atoll
make build            # -> bin/atoll, bin/atoll-server, bin/atoll-daemon
```

### 2. Install and run a node

First time — the interactive installer. It preflights (previous install, codex/claude
CLIs and their login state, free port, writable home), asks you to pick the c0
steward, sets the root password, writes `<home>/atoll.env`, then runs `atoll up`
and prints how to log in:

```bash
scripts/install.sh
```

Non-interactive (accept every default; good for CI / scripted setups):

```bash
scripts/install.sh --yes
# honours ATOLL_HOME / ATOLL_ADDR / ATOLL_ROOT_PASSWORD / ATOLL_STEWARD
```

Every later start is just the node itself — it reads the `atoll.env` the installer
wrote, so a bare `atoll up` reopens the same instance (default home `~/.atoll`):

```bash
bin/atoll up                      # same as: bin/atoll up --dir ~/.atoll
bin/atoll up --dir ~/.atoll --addr 127.0.0.1:8832   # flags still win over atoll.env
```

What the installer leaves behind:

```
~/.atoll/
├── atoll.env                 # ATOLL_ADDR / ATOLL_STEWARD — the memory of the install
├── atoll-up.log              # node log (JSON lines)
├── server/
│   ├── atoll-token           # bearer token for local automation (0600)
│   ├── root-password         # if you set/generated one (0600)
│   ├── registry.db           # space registry
│   └── channels/             # one SQLite ledger per channel
└── device/                   # the local device's identity + working dirs
```

Check it is alive:

```bash
curl -s http://127.0.0.1:8832/healthz      # {"status":"ok"}
```

Account: `root` (`root@atoll.local` spelled out also works), with the password you chose (or the generated one in
`~/.atoll/server/root-password`). Stop with `Ctrl-C`; the node is single-homed, so
a second `atoll up` on the same `--dir` is refused by a lock.

### 3. Open the web UI

The browser interface is in the node, on the same port as the API: open
`http://127.0.0.1:8832`. It reaches `/api`, `/ws`, `/obs` and `/files` over
relative paths and is same-origin with them, so plain cookie auth works — no
proxy, no CORS.

Log in as `root` (accounts the node carved need no domain), you are in `c0` with the steward; mention it and the
answer streams back as a round in the timeline.

**From a source checkout** `web/dist` holds only a placeholder page, which says as
much when you open it. To build the real one in:

```bash
make web       # builds the atoll-web tag named in WEB_VERSION into web/dist
make build     # bin/atoll now carries that UI
```

**While working on the front end** there is no need to rebuild each time — run
vite, which proxies `/api`, `/ws`, `/obs` and `/files` to the node:

```bash
git clone git@github.com:wanpengxie/atoll-web.git
cd atoll-web && npm install
npm run dev                   # -> http://localhost:5173
```

If the node listens somewhere else, point the dev proxy at it (this only affects
the proxy, it never enters the browser bundle):

```bash
ATOLL_SERVER_URL=http://127.0.0.1:9000 npm run dev
```

`atoll-web` also ships a stand-alone mock of the node (`npm run mock`) if you want
to try the UI without a node.

```
 browser ──► atoll up (:8832)
             ├── web UI       (static pages shipped in the binary, same origin)
             ├── server       (ledgers: c0, channels, registry)
             └── local device (runs codex/claude/tools)
```

### Running the roles as separate processes

`atoll up` is sugar over two binaries that share the same on-disk homes, so a node
started with `atoll up` can be split later with zero migration:

```bash
# server — holds the ledgers for all channels; installs itself on first run
# (the generated root password is printed to the log)
bin/atoll-server --home ~/.atoll/server --addr 127.0.0.1:8832

# daemon — a compute host (a "device"); first run binds with a device key (minted by
# `atoll up` provisioning or via system.device.create); later runs start bare,
# the identity persists in the home
bin/atoll-daemon --home ~/.atoll/device --server "ws://127.0.0.1:8832/compute" --key <device-key>
```

Reach for this only when you actually want the roles on different machines.

## How a node is put together

After `atoll up` you have, concretely:

- **One space, one server.** Truth lives in `~/.atoll/server/` as SQLite files —
  one ledger per channel plus a registry. Nothing is in memory only.
- **A channel tree rooted at `c0`.** Channels are addressed by dotted name:
  `c0`, `c0.lobby`, `c0.dev`, `c0.<user>` (each registered user's home). `c0` is
  where the space is administered — channels, agent/tool templates, devices and
  accounts are all created by speaking to `system` in `c0`. `c0.lobby` is the one
  room outside the trust boundary: it exists only so guests can register and log in.
- **Actors with three-segment ids** `<kind>:<seed>:<ts>` — `human:root:…`,
  `agent:reviewer:…`, `tool:my-mcp:…`, `system:registrar:…`, `peer:c0.dev:…`. The
  kind is the namespace (`tool / human / agent / peer / system`); the seed is the
  template id (or the principal, for humans); you may address an actor by any
  unambiguous run of segments (`reviewer`, `agent:reviewer`). The word `system`
  alone is the channel's gate. The roster of a channel is `/obs/channel/<id>/actors`.
- **Inside every channel:** a gate (`system`) that handles membership and routes
  space-level words to `c0`; a service actor that answers for the channel to other
  channels (`peer:*`); whatever members you seat there.
- **In `c0` additionally:** `registrar` (the only writer of the registry), root's
  human cell, and the **steward** — the codex or claude CLI you picked, running as
  an agent actor on your device, under your own CLI login.
- **Devices.** A device is a machine that runs actors. `atoll up` attaches the local
  one automatically; `atoll-daemon` attaches more. Agents run *on a device* with a
  working directory there — that is where their shell, git and files are.
- **An account** `root@atoll.local` (root's home is `c0` itself) and a bearer token
  in `~/.atoll/server/atoll-token` so scripts on the same box need not log in.

## Using the node

### From the web UI

`atoll-web` (step 3) is a client over the node's public contract — login, `/ws`
frames, `/obs` reads — nothing more. Channels on the left, the channel's timeline
(people, agent rounds, system events) in the middle, the roster on the right; the
composer sends `agent.ask` to an agent or `human.message` to people. Everything
it shows is read from the ledger; everything it does is a message on it.

### From a script

Authenticate with the local token:

```bash
TOKEN=$(cat ~/.atoll/server/atoll-token)
AUTH="Authorization: Bearer $TOKEN"
```

The node exposes exactly three surfaces:

| Surface | Method | What it is |
|---|---|---|
| `/obs/...` | `GET` | read-only observation of the space and channels |
| `/ws` | WebSocket | the only way to send anything into a channel |
| `/files/<address>?t=<ticket>` | `GET` / `PUT` | file data plane; tickets are issued over `/ws` |

**Reads** are one curl each:

```bash
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/channels
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/daemons      # devices
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/principals   # accounts
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/space/decls        # actor templates
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/channel/c0/profile
curl -s -H "$AUTH" http://127.0.0.1:8832/obs/channel/c0/actors
```

Answers are `{subject, kind, complete, items[]}`; each item shows what the ledger
says (`declared`) next to what was just observed (`actual`). A fact that could not
be observed comes back as `unknown` with a reason, never as a made-up `false`.

**Writes** go over the WebSocket: open it with the same bearer token, send one
`attach` frame, then one `submit` frame per request. Receipts and every other
member's messages arrive on the same connection, so keep it open for the session.

```jsonc
// 1. subscribe (replay from the beginning; pass a cursor to resume)
{"v":2,"frame_type":"attach","ref":"a","payload":{"since":{}}}

// 2. ask the c0 gate to list members
{"v":2,"frame_type":"submit","ref":"r1","payload":{
  "channel_id":"c0","msg_type":"system.member.list","kind":"request",
  "visibility":"public","audience":["system"],"payload":{}}}

// 3. ask the steward something (an audience entry may omit id segments:
//    "steward" resolves against the roster's agent:steward:<ts>)
{"v":2,"frame_type":"submit","ref":"r2","payload":{
  "channel_id":"c0","msg_type":"agent.ask","kind":"request",
  "visibility":"public","audience":["steward"],
  "payload":{"text":"reply PONG"}}}
```

A submit frame carries the bare arguments; the ledger wraps them into the one
request shape `{"body": <args>}`, which is what you see on the feed. The receipt carries
`payload.message_id`; the answer arrives later as a `feed` frame with
`kind:"response"`, `parent_id` equal to that id and a terminal `status` of
`completed` or `failed`. An agent's rounds (`agent.turn.*`, `agent.tool.*`) show
up on the same feed as events before its final response.

**Administration is a vocabulary, not an admin panel.** All of it is requests to
`system` (the gate); space-level words are spoken in `c0`, channel-level words in
the channel itself:

| Family | Words | Spoken in |
|---|---|---|
| channels | `system.channel.create/get/list/set/delete`, `system.channel.template.*` | `c0` |
| agent & tool classes | `system.actor.template.create/get/list/set/delete`, `system.actor.overlay.set/delete` | `c0` |
| machines | `system.device.create/list/attach/detach/delete` | `c0` |
| accounts | `system.principal.create/get/list/delete` | `c0` |
| membership | `system.member.create/get/list/delete/restart/admit` | the channel |
| secrets | `system.credential.set` | the channel |
| logs | `system.log.recent` | the channel |

Agents answer `agent.ask / steer / interrupt / queue / stop / compact / select /
context / fork`; people answer `human.message / ask / approve`; every actor answers
`actor.describe` (its manifest: class, capabilities, words). Full shapes are the Go
structs in [`protocol/message/system.go`](protocol/message/system.go) and
[`platform/lagoon/contracts.go`](platform/lagoon/contracts.go).

### Recipes

**Seat a second agent.** Agents are *templates* (a class — `codex`, `claude`,
`script` — plus config) that you then *seat* in a channel; one template can have
many instances unless declared `singleton`:

```jsonc
// in c0, to system — declare the template
// id is the key; name is what the seated member will be called (lowercase
// a-z, 0-9, '-'), and it becomes the middle segment of its actor id
{"msg_type":"system.actor.template.create","payload":{
  "id":"reviewer","name":"reviewer","description":"Reviews changes.",
  "class":"claude","visibility":"private",
  "singleton":false,"config":{/* class-specific */}}}

// in the target channel, to system — seat one instance as agent:reviewer:<ts>
{"msg_type":"system.member.create","payload":{"decl_id":"reviewer"}}
```

**Mount an MCP server — two messages, no rebuild.**

```jsonc
// in c0, to system
{"id":"my-mcp","name":"my-mcp","description":"My MCP server.",
 "class":"mcp","visibility":"private","singleton":false,
 "config":{"name":"testsrv","transport":"http","url":"http://127.0.0.1:8931/mcp"}}

// in the channel, to system   -> {"status":"completed","member":"tool:my-mcp:<ts>"}
{"decl_id":"my-mcp"}
```

The actor connects on birth and discovers the server's `tools/list`; each tool
becomes a message type prefixed by the `name` you declared (`testsrv.echo`,
`testsrv.add`, …), and `actor.describe` on it lists them. Use `transport:"stdio"`
with `command` / `args` / `cwd` instead of `url` for a local subprocess. Anyone in
the channel — a script, a person, another actor — can then `submit` `testsrv.echo`
to it.

**Create a channel and give it an agent.** `system.channel.create {name, recipe}`
in `c0`; the recipe lists the templates to seat. The new channel is carved in one
step from the frozen recipe and shows up in `c0`'s roster as
`peer:<qualified name>:<ts>` — a handle you can speak to like any other member.

**Attach another machine as a device.** Ask `system.device.create` in `c0` for a
key, then on the other box:

```bash
bin/atoll-daemon --home ~/.atoll/device --server "ws://<node>:8832/compute" --key <device-key>
```

It shows up in `/obs/space/daemons` and can host agents and tools like the local one.

## Design notes

The properties below are structural (enforced by how the code is shaped, checked
by `archtest/`), not conventions. The reasoning is in
[docs/architecture](docs/architecture/README.md).

- **Ledger is truth.** Appended before anyone sees it; no side channel, no REST
  "send"; replayable, recoverable, forkable.
- **Identity outlives the incumbent.** Restart, upgrade or swap the model process;
  it is the same member. Providers plug in by writing usage/steps as events and
  honouring the stop protocol.
- **Permission on the membrane, not in the prompt.** Gate decides at the channel
  boundary, uniformly; no token impersonation.
- **Absence never destroys.** One convergence loop; no cascading kills, no hidden
  timers.
- **Organisation layer = protocol + a service member.** Jobs, approvals, quotas,
  interposition install by adding a member to a channel (Slack "add app" style);
  zero OS changes. Designed, not shipped.
- **People are members.** An approval is a message to a person; the web UI is one
  client of the same contract.
- **Self-managed by the same law.** `c0` governs channels through the same gate;
  the next version is built in a forked Atoll before it is switched in.

## Extending: write your own actor

Anything that lives in a channel — agent engine, tool, adapter — is an **actor**:
a Go function over a small verb table. `Sys` is handed to it at birth and is its
only way to touch the world — receiving is `Recv`, answering is `Reply` or `Fail`.
Sender identity is welded on; it is not a field the actor fills in. This is the
real echo actor, whole:

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
// drivers/tools/echo/register.go — one registry entry: config → running actor
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

Register a class like that, rebuild, and `system.actor.template.create` with
`"class":"echo"` makes it available to every channel. The shipped engines
(`codex`, `claude`, `script`) and tools (`echo`, `mcp`, `device`, `kimi`, `xhs`)
are built exactly this way under `drivers/`; a step-by-step walkthrough is in
[docs/architecture/09-actor-hello-world.md](docs/architecture/09-actor-hello-world.md).

## Repository layout

```
cmd/         binaries: atoll (the node), server, daemon; + devtools and shared internals
scripts/     install.sh (interactive installer) and repo lint scripts
drivers/     the outside world: tools/ (echo, device, kimi, mcp, xhs),
             agents/ (engine providers: codex, claude, script),
             gateway/ (human ingress; portal/ = identity doors + /ws + /obs + /compute),
             devicehost/ (the local device `atoll up` connects)
protocol/    wire vocabulary (envelope, actor ids, channel peer frames, access, resource, system words)
runtime/     the core: admission pipeline, actor cells/ports, sqlite ledgers, actor store,
             schedule/timers, access door
lib/         stdlib for actor authors: actorbase (the Proc + verb-table base every
             actor stands on), behavior, metatool (tool catalog), introspect
platform/    assembly: channelhost/, peeractor/ + svcactor/ (cross-channel path),
             home/ (server-side channel home), daemonhost/ + compute/ (device side),
             subjectgate/, lagoon/ (registry + registrar), boot/ (installer)
registry/    actor class registry (config → running actor)
archtest/    architecture enforcement tests (layer graph + closed sets)
e2e/         end-to-end tests over the real server + daemon binaries
mcp-testserver/  a small MCP server used by tests and the MCP recipe
docs/        architecture series, dev walkthroughs, product notes
atoll-site/  project site (Jekyll)
```

## Development

```bash
make build          # all three binaries into bin/
make test           # day-to-day: -short -race, sqlite fsync off (tag atolltestfast)
make test-full      # gate before merging to main: the same, nothing skipped
make lint           # go vet + architecture tests + repo lint scripts
make e2e-loop       # black-box acceptance: two real OS processes over the portal wire
make dev            # API-only server on :8832 with home /tmp/atoll-dev (for atoll-web work)
```

Tests are organised per package; when touching one package, run that package
(`go test -race ./runtime/actorrt/`) rather than the whole tree. The architecture
tests in `archtest/` are part of `make lint` and are meant to fail when a layering
invariant is broken — read the header comment of a failing test before changing it.

## Documentation

- [docs/architecture/](docs/architecture/README.md) — why an agent loop is not an
  agent OS, the five primitives, channels as autonomous worlds, actors, scheduling,
  federation, roadmap, and an actor hello-world.
- [docs/dev/](docs/dev/README.md) — developer walkthroughs of the substrate,
  actorbase, composition, and the design notes behind current constructions.
- [docs/production/](docs/production/README.md) — positioning, competitor notes,
  adoption strategy.
- [docs/credential-system-walkthrough.md](docs/credential-system-walkthrough.md) —
  how credentials move through the access plane.
- [`atoll-web`](https://github.com/wanpengxie/atoll-web) — the browser client and
  its contract notes (including a stand-alone mock of the node).

## Status

**v0.01, pre-release.** The core (identity, channels, ledger, membership, devices,
timers) is implemented and enforced and the project is used to develop itself;
the organisation layer is next. Until 1.0:

- **No data guarantees** — no upgrade or migration path; schemas, homes and the
  wire dialect change; a newer binary may refuse or re-carve an older `~/.atoll`
  (the installer offers to move it aside).
- **No deprecation cycles** — message types, frames and Go package surfaces move;
  the web client and the node must move together (a mismatch shows as
  `type_unsupported` or a rejected frame).
- **Boundaries** — one trust domain per node; `atoll up` manages only the local
  device, others are attached by hand. Gaps: see [What works today](#what-works-today-2026-08).

## License

[Apache-2.0](LICENSE). The Atoll name and any hosted offering are separate from the
code license.

## Name

The project is **atoll** (lowercase); the Go module is
`github.com/wanpengxie/atoll`. An atoll is a reef ring built by countless small
organisms depositing layer upon layer — no one owns the reef, and it grows by
sedimentation. That is the design.
