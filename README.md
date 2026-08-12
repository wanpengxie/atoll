# Atoll

**A kernel for agent collaboration.**

Atoll is a substrate where AI agents, humans, and tools work together as **actors** in
shared **channels** — on top of an enforced, append-only **truth log** for everything
they say, and membership-gated **access** for everything they touch.

It is built kernel-first: the guarantees are structural, not conventions. An actor
cannot forge another actor's messages, cannot write around the log, and cannot reach
past its channel's membrane — not because a prompt asked it nicely, but because the
geometry does not compile.

## Why a kernel

Every serious multi-agent system ends up hand-building the same checklist: durable
state that survives restarts, stable identity across crashes and respawns, an audit
trail of who did what, authorization for tools and credentials, idempotent recovery,
lifecycle cleanup. Actor frameworks keep agents *alive*; nothing in that stack keeps
them *truthful*.

Atoll's position is that this checklist is not application work — it is an operating
system's work. Agents are the new processes; they need a kernel.

## The minimal kernel: one boundary, five elements, two planes

```
                 channel (the boundary)
   ┌──────────────────────────────────────────────┐
   │   actor  ──── message ────▶  actor            │   horizontal plane:
   │  (subject)   (into the truth log)  (subject)  │   collaboration = truth
   │     │                                         │
   │     ├────── access ────▶  resource            │   vertical plane:
   │     │  (membership-gated,  (object)           │   reaching data = authority
   │     │   off-log)                              │
   │     └────── timer ─────▶  future self         │   time as a cause:
   │        (durable promise,   (wake)             │   wakes survive restarts
   │         fires as delivery)                    │
   └──────────────────────────────────────────────┘
```

| | What it is |
|---|---|
| **channel** | The boundary: one execution domain, one trust boundary, one data boundary. The five elements live inside it. |
| **actor** | A subject: agent, human, or tool. Addressed by identity; alive as an incarnation. |
| **resource** | An object: passive owned data (state, files, secrets), opaque to the kernel. |
| **message** | Subject ↔ subject. Appended to the channel's truth log, delivered from its tail. |
| **access** | Subject ↔ object. Off-log, checked at a gate: membership admits, ownership distinguishes. |
| **timer** | Subject ↔ future time. A durable promise to wake an actor at a moment — it survives restarts, and firing is a delivery, not a poll. |

Remove any element and the system stops working; anything more is domain. Messages
are the IPC of this world; access is its syscall; timers are its cron — the one
cause with no sending actor, which is exactly why it must be a kernel element
rather than every agent's hand-rolled polling loop.

## What the kernel enforces

- **The server is truth.** Every message is appended to a per-channel log (SQLite
  today) before delivery; delivery *is* the log tail. There is no side channel.
- **Writes are welded to identity.** An actor writes through a *pen* minted at birth;
  sender identity is not a field it fills in, so it is not a field it can forge.
- **The membrane is the permission.** Inside a channel, every member reads and writes
  alike; the only distinctions are structural — who created a thing, who owns the
  channel. There are no per-object grants or ACLs to administer, so there are none
  to drift.
- **Admission is a pipeline.** Every write passes a fixed chain of checks — caller
  authorization, envelope shape, sender consistency, kind/audience rules, response
  pairing, dedupe — before it becomes truth.
- **Identity and liveness are two different things.** An actor's *identity* is stable
  (addressing, membership, the log); each activation is a distinct *incarnation*
  (lifecycle, capability validity). Dead incarnations cannot haunt the log.
- **Requests always close.** A request's terminal state has three authors: the actor's
  reply, the caller's timeout, or the actor's death. No dangling futures.
- **Transport is neutral.** An in-process actor (cell) and a remote one (port over
  WebSocket) are indistinguishable to the runtime — same contract, same gates.
- **The layering is machine-checked.** Architecture tests fail the build when domain
  code reaches into kernel internals. The rules hold for agent-written code too —
  that is the point.

## How it relates to MCP and A2A

They are complements, not competitors. **MCP** connects one agent to its tools.
**A2A** connects two agents pairwise. Message-bus approaches add a shared space for
many agents. Atoll's concern is the layer none of them claim: a **shared truth** of
what was said, **enforced identity** for who said it, and an **authority plane** for
what each participant may touch. Adapters for existing ecosystems (MCP servers,
CLIs, coding-agent harnesses) attach as ordinary actors at the boundary.

## Repository layout

```
protocol/    protocol types (envelope, actor, channel, access, resource)
runtime/     the kernel runtime (harness admission pipeline, actorrt cells/ports,
             sqlite store, actor store, schedule/timers, access door)
lib/         stdlib for actor authors: actorbase (the Proc + verb-table base every
             actor stands on), behavior, metatool (tool catalog / call_actor
             vocabulary), introspect
platform/    cross-host membrane (ActorDecl + ActorFactory, the shared word table);
             channelhost/ (the channel contract surface the space talks to),
             spacetool/ (in-channel port for space requests), home/ (server-side
             channel-home assembly), daemonhost/ (space device carriers),
             compute/ (daemon-side multi-compartment assembly), subjectgate/,
             lagoon/ (registry storage module + registrar), boot/ (one-shot
             installer, sole registry DDL owner)
drivers/     external-world drivers: tools/ (echo, device, kimi, xhs),
             agents/ (agent engine providers: codex, script),
             gateway/ (human ingress; portal/ = identity doors + /ws + /compute)
registry/    actor class registry (config → running actor)
cmd/         binaries (server, daemon) + devtools
archtest/    architecture enforcement tests (layer graph + closed sets)
e2e/         end-to-end tests over the real server + daemon binaries
docs/        architecture and dev walkthroughs
```

## Quickstart

```bash
# 1. build
make build          # -> bin/atoll, bin/atoll-server, bin/atoll-daemon

# 2. one-command personal node: engine + owner + home channel + local device,
#    all provisioned and converging on every run (default home: ~/.atoll)
bin/atoll up
```

Or run the roles as separate processes on the SAME homes `atoll up` uses —
the disk layout is identical, so a node started with `atoll up` can be split
later with zero migration:

```bash
# server (holds truth for all channels; installs itself on first run — the
# generated root password is printed to the log)
bin/atoll-server --home ~/.atoll/server --addr 127.0.0.1:8832

# daemon (compute host) — first run binds with a device key (minted by
# `atoll up` provisioning, or via the device.mint space word); later runs
# start bare (identity persists in the home)
bin/atoll-daemon --home ~/.atoll/device --server "ws://127.0.0.1:8832/compute" --key <device-key>

# tests
make test
```

The web UI lives in a separate repository and talks to the same portal API.

## Writing an actor

An actor is a function over a small verb table. `Sys` is handed to it at birth and
is its only way to touch the world — receiving is `Recv`, answering is `Reply` or
`Fail`. Every write carries the actor's welded identity; sender is not a field it
can set. This is the real echo actor, whole:

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

## Status

Atoll is **v0.01 — a working minimal kernel, pre-release**. The five elements and
both planes are in place and enforced; the developer shell around them (one-command
setup, coding-agent connectors, scaffolding) is being built next. Known, deliberate
boundaries at this stage: single trust domain per deployment; observer read access is
a revocable per-channel space capability, while members retain intrinsic read access.
APIs still move without deprecation cycles. Kernel first, polish second — watch the
repo if you want to see the rest arrive.

## License

[Apache-2.0](LICENSE). The Atoll name and any hosted offering are separate from the
code license.

## Name

The project is **atoll** (lowercase); the Go module is
`github.com/wanpengxie/atoll`. An atoll is a reef ring built by countless small
organisms depositing layer upon layer — no one owns the reef, and it grows by
sedimentation. That is the design.
