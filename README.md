# Atoll

English | [简体中文](README.zh-CN.md)

**A kernel for agent collaboration.**

Atoll is a self-hosted node where your coding agents (codex, claude, …), MCP tools
and you work in shared **channels**. Everything anyone says in a channel goes into
one append-only log the server owns; who said it is enforced, not declared; what an
actor may touch is decided by channel membership, not by per-tool config. You run
it on your own machine, talk to it from a browser or a script, and add agents,
tools and machines to it at runtime — no rebuild.

It is built kernel-first: the guarantees are structural, not prompts. The
reasoning behind that is in [docs/architecture](docs/architecture/README.md);
this README is about installing and using it.

> **Status: v0.01 — pre-release, not data-safe.**
> Atoll runs end to end today (`atoll up` + the web UI), but it is an early kernel,
> not a product. **Storage formats, APIs and the wire protocol change without
> deprecation cycles or migration paths** — a newer build may refuse or overwrite a
> node home created by an older one. Do not keep anything in it you cannot recreate.
> See [Status](#status).

---

- [Quickstart](#quickstart)
  - [1. Build](#1-build)
  - [2. Install and run a node](#2-install-and-run-a-node)
  - [3. Start the web UI](#3-start-the-web-ui)
  - [Running the roles as separate processes](#running-the-roles-as-separate-processes)
- [What you get](#what-you-get)
- [Using the node](#using-the-node)
  - [From the web UI](#from-the-web-ui)
  - [From a script](#from-a-script)
  - [Recipes](#recipes)
- [Extending: write your own actor](#extending-write-your-own-actor)
- [Concepts you will meet](#concepts-you-will-meet)
- [Repository layout](#repository-layout)
- [Development](#development)
- [Documentation](#documentation)
- [Status](#status)
- [License](#license)

---

## Quickstart

A complete local setup is three steps: build the binaries, run the installer once,
start the web UI against the node. You end up with a running node at
`http://127.0.0.1:8832`, a `root` account, and a coding agent (codex or claude)
already seated in the root channel `c0` as its **steward** — open the UI, log in,
and talk to it.

**Prerequisites**

| For | Need |
|---|---|
| the node | Go 1.25+ ([go.mod](go.mod)), `make`, `curl` |
| a steward agent | [`codex`](https://github.com/openai/codex) and/or [`claude`](https://github.com/anthropics/claude-code) CLI installed **and logged in** (the installer detects both and lets you pick; you can add one later) |
| the web UI | Node.js 22+ and the sibling repo [`atoll-web`](https://github.com/wanpengxie/atoll-web) |

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

`atoll up` is the whole node in one process: it installs or opens `c0`, provisions
the root principal and the home channel, binds the listener, and connects the
well-known local device (the compute host that actually runs agents and tools). You
do not mint or attach a device by hand.

What the installer leaves behind:

```
~/.atoll/
├── atoll.env                 # ATOLL_ADDR / ATOLL_STEWARD — the memory of the install
├── atoll-up.log              # node log (JSON lines)
├── server/
│   ├── atoll-token           # bearer token for local automation (0600)
│   ├── root-password         # if you set/generated one (0600)
│   ├── registry.db           # space registry
│   └── channels/             # one SQLite truth log per channel
└── device/                   # the local device's identity + working dirs
```

Check it is alive:

```bash
curl -s http://127.0.0.1:8832/healthz      # {"status":"ok"}
```

Account: `root@atoll.local` with the password you chose (or the generated one in
`~/.atoll/server/root-password`). Stop with `Ctrl-C`; the node is single-homed, so
a second `atoll up` on the same `--dir` is refused by a lock.

### 3. Start the web UI

The browser client is a separate repository and is **not** served by the node. It
is a Vite app that proxies `/api`, `/ws`, `/obs` and `/files` to the node, so the
browser always sees one origin and plain cookie auth works.

```bash
# in a sibling directory, with the node from step 2 running on :8832
git clone git@github.com:wanpengxie/atoll-web.git
cd atoll-web
npm install
npm run dev                   # -> http://localhost:5173
```

If the node listens somewhere else, point the dev proxy at it (this only affects
the proxy, it never enters the browser bundle):

```bash
ATOLL_SERVER_URL=http://127.0.0.1:9000 npm run dev
```

Open the printed URL, log in as `root@atoll.local`, pick `c0`, and mention the
steward (`@steward …`) from the editor — the reply streams back as rounds in the
timeline. `atoll-web` also ships a stand-alone mock of the same contract
(`npm run mock`) if you want to try the UI without a node; see its README.

Putting the three together on one machine looks like:

```
 browser ──► atoll-web (vite :5173) ──proxy──► atoll up (:8832)
                                               ├── server   (truth: c0, channels, registry)
                                               └── local device (runs codex/claude/tools)
```

### Running the roles as separate processes

`atoll up` is sugar over two binaries that share the same on-disk homes, so a node
started with `atoll up` can be split later with zero migration:

```bash
# server — holds truth for all channels; installs itself on first run
# (the generated root password is printed to the log)
bin/atoll-server --home ~/.atoll/server --addr 127.0.0.1:8832

# daemon — a compute host; first run binds with a device key (minted by `atoll up`
# provisioning or via the system.device.create space word); later runs start bare,
# the identity persists in the home
bin/atoll-daemon --home ~/.atoll/device --server "ws://127.0.0.1:8832/compute" --key <device-key>
```

Reach for this only when you actually want the roles on different machines.

## What you get

After `atoll up` you have, concretely:

- **A node** on `127.0.0.1:8832` holding one space. Truth lives in
  `~/.atoll/server/` as SQLite files; nothing is in memory only.
- **A root channel `c0`** and a `lobby` under it. `c0` is where the space is
  administered — new channels, agents, devices and accounts are all created by
  speaking to `system` in `c0`.
- **An account** `root@atoll.local` (the only one until you create more or open
  registration with `--open-registration`).
- **A steward** — the codex or claude CLI you picked, running as an agent actor
  inside `c0`, on your machine, under your own CLI login. Mention it and it works
  a request, streams its rounds into the channel, and can call the tools mounted
  next to it.
- **A local device** — the compute host that actually runs agents and tools. It is
  attached automatically; more machines can join as extra devices.
- **A bearer token** in `~/.atoll/server/atoll-token` so scripts on the same box
  can talk to the node without logging in.

Everything a channel's members say or do is one ordered log per channel. The
web UI, the agents and your scripts all read the same log and write through the
same gate; there is no privileged back door for any of them.

## Using the node

### From the web UI

`atoll-web` (step 3 above) is a plain chat-style client over the node's contract:

- **Channels** on the left: `c0`, `lobby`, and whatever you create. Each shows
  its timeline (human messages, agent rounds, system events) and its roster.
- **Talk to an agent** by `@`-mentioning it in the editor. Its answer arrives as a
  round — queued → processing → tool calls → final text — all of which are
  messages in the log, so you see exactly what it did.
- **Approvals**: when an agent asks for something that needs a human (a credential,
  a risky action), a card appears in the channel; approve or reject from there.
- **Roster**: who is in the channel — humans, agents, `system`, tools — and what
  each one can be asked (`actor.describe`).

There is nothing the UI can do that a script cannot; it uses the same three
surfaces below.

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

// 2. ask `system` in c0 to list members
{"v":2,"frame_type":"submit","ref":"r1","payload":{
  "channel_id":"c0","msg_type":"system.member.list","kind":"request",
  "visibility":"public","audience":["system"],"payload":{"body":{}}}}
```

The receipt carries `payload.message_id`; the answer arrives later as a `feed`
frame with `kind:"response"`, `parent_id` equal to that id and a terminal
`status` of `completed` or `failed`. To talk to an agent the way the web UI
does, submit `msg_type:"human.text"` with the agent in `audience`; its rounds
(`activity.turn.*`, `activity.tool.*`) and final answer come back on the feed.

**Administration is a vocabulary, not an admin panel.** All of it is requests to
`system` (templates go to `system:registrar`):

| Family | Words | Spoken in |
|---|---|---|
| channels | `system.channel.create/get/list/set/delete`, `system.channel.template.*` | `c0` |
| agents & tools (classes) | `system.actor.template.create/get/list/set/delete`, `system.actor.overlay.set/delete` | `c0` |
| membership | `system.member.create/get/list/delete/restart/admit` | the channel itself |
| machines | `system.device.create/list/attach/detach/delete` | `c0` |
| accounts | `system.principal.create/get/list/delete` | `c0` |
| secrets | `system.credential.set` | the channel itself |
| logs | `system.log.recent` | the channel itself |

Full request/response shapes are the Go structs in
[`protocol/message/system.go`](protocol/message/system.go) and
[`platform/lagoon/contracts.go`](platform/lagoon/contracts.go).

### Recipes

**Add a second agent to a channel.** Agents are *templates* (a class — `codex`,
`claude`, `script` — plus config) that you then *seat* in a channel:

```jsonc
// in c0, to system:registrar — declare the template
{"msg_type":"system.actor.template.create","payload":{"body":{
  "id":"reviewer","name":"Reviewer","class":"claude","visibility":"private",
  "singleton":false,"config":{/* class-specific */}}}}

// in the target channel, to system — seat one instance
{"msg_type":"system.member.create","payload":{"body":{"decl_id":"reviewer"}}}
```

**Mount an MCP server — two messages, no rebuild.**

```jsonc
// in c0, to system:registrar
{"id":"my-mcp","name":"My MCP","class":"mcp","visibility":"private",
 "singleton":false,
 "config":{"name":"testsrv","transport":"http","url":"http://127.0.0.1:8931/mcp"}}

// in the channel, to system   -> {"status":"completed","member":"tool:my-mcp:<ts>"}
{"decl_id":"my-mcp"}
```

The actor connects on birth and discovers the server's `tools/list`; each tool
becomes a message type prefixed by the `name` you declared (`testsrv.echo`,
`testsrv.add`, …), and `actor.describe` on it shows them. Use
`transport:"stdio"` with `command` / `args` / `cwd` instead of `url` for a local
subprocess. Agents in the same channel can call these tools.

**Attach another machine as a device.** Ask `system.device.create` in `c0` for a
key, then on the other box:

```bash
bin/atoll-daemon --home ~/.atoll/device --server "ws://<node>:8832/compute" --key <device-key>
```

It shows up in `/obs/space/daemons` and can host agents and tools like the local one.

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

## Concepts you will meet

Five words cover the whole system; everything else is built from them.

| Word | Means |
|---|---|
| **channel** | A room with one log, one membership, one set of files. `c0` is the root; others are created under it. |
| **actor** | Anything that is a member of a channel and can speak: a human, an agent, a tool, `system`. Addressed by a stable id; each run of it is a separate incarnation. |
| **message** | One entry in a channel's log. Requests get exactly one terminal response — from the actor, from a timeout, or from the actor dying. |
| **access** | An actor reaching a file, a secret, a piece of state. Allowed by membership in the channel that owns it; not logged, but gated. |
| **device** | A machine that runs actors for the node. `atoll up` gives you one; `atoll-daemon` adds more. |

Things the node guarantees that you can rely on when building on it:

- **The server is the only writer.** Every message is appended to the channel's
  log before anyone sees it; there is no side channel and no REST "send".
- **Sender cannot be forged.** An actor writes through a pen minted at birth.
- **Membership is the permission.** Inside a channel, members read and write alike;
  there are no per-object ACLs to administer or to drift.
- **Every write passes the same admission chain** — caller authorization, envelope
  shape, sender consistency, kind/audience rules, response pairing, dedupe.
- **Dead incarnations cannot haunt the log**; a restarted agent is a new incarnation
  of the same identity.
- **Local and remote actors are indistinguishable** to the runtime.
- **Layering is machine-checked** (`archtest/`) — it holds for agent-written code
  too.

Why it is shaped this way, and how it relates to MCP, A2A and message buses, is
the subject of the [architecture series](docs/architecture/README.md).

## Repository layout

```
cmd/         binaries: atoll (the node), server, daemon; + devtools and shared internals
scripts/     install.sh (interactive installer) and repo lint scripts
drivers/     external-world drivers: tools/ (echo, device, kimi, mcp, xhs),
             agents/ (agent engine providers: codex, claude, script),
             gateway/ (human ingress; portal/ = identity doors + /ws + /obs + /compute),
             devicehost/ (the local device `atoll up` connects)
protocol/    protocol types (envelope, actor, channel, access, resource, system words)
runtime/     the kernel runtime (admission pipeline, actor cells/ports, sqlite store,
             actor store, schedule/timers, access door)
lib/         stdlib for actor authors: actorbase (the Proc + verb-table base every
             actor stands on), behavior, metatool (tool catalog), introspect
platform/    cross-host assembly: channelhost/, peeractor/ + svcactor/ (cross-channel
             path), home/ (server-side channel home), daemonhost/ + compute/ (device
             side), subjectgate/, lagoon/ (registry + registrar), boot/ (installer)
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

- [docs/architecture/](docs/architecture/README.md) — why it is a kernel and not
  an agent loop, the five elements, channels, actors, scheduling, federation,
  roadmap, and an actor hello-world.
- [docs/dev/](docs/dev/README.md) — developer walkthroughs of the substrate,
  actorbase, composition, and the design notes behind current constructions.
- [docs/production/](docs/production/README.md) — positioning, competitor notes,
  adoption strategy.
- [docs/credential-system-walkthrough.md](docs/credential-system-walkthrough.md) —
  how credentials move through the access plane.
- [`atoll-web`](https://github.com/wanpengxie/atoll-web) — the browser client and
  its contract notes (including a stand-alone mock of the node).

## Status

Atoll is **v0.01 — pre-release**. It runs end to end: a node installs and starts
in one command, a coding agent sits in `c0` as steward, MCP servers mount at
runtime, and the web UI talks to it over the public contract.

What that version number means in practice:

- **No data guarantees.** There is no upgrade or migration path before 1.0. SQLite
  schemas, the registry, on-disk homes and the wire dialect all change; a newer
  binary may refuse an older home or silently re-carve it. Treat `~/.atoll` as
  disposable — the installer offers to move a previous home aside for exactly this
  reason.
- **APIs move without deprecation cycles.** Message types, frame shapes and Go
  package surfaces are revised as the kernel is reviewed; nothing is frozen.
- **Current boundaries:** one trust domain per node; the local device is the only
  compute host `atoll up` manages for you (others are attached by hand); the
  shipped engines are codex, claude and a generic `script` runner.

Kernel first, polish second — more connectors, scaffolding and packaging come next.
Watch the repo if you want to see the rest arrive.

## License

[Apache-2.0](LICENSE). The Atoll name and any hosted offering are separate from the
code license.

## Name

The project is **atoll** (lowercase); the Go module is
`github.com/wanpengxie/atoll`. An atoll is a reef ring built by countless small
organisms depositing layer upon layer — no one owns the reef, and it grows by
sedimentation. That is the design.
