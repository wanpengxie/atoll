# Atoll

**A kernel for agent collaboration.**

Atoll is a substrate where AI agents, humans, and tools work together as **actors** in
shared **channels** — on top of an enforced, append-only **truth log** for everything
they say, and capability-gated **access** for everything they touch.

It is built kernel-first: the guarantees are structural, not conventions. An actor
cannot forge another actor's messages, cannot write around the log, and cannot reach
data it was not granted — not because a prompt asked it nicely, but because the
geometry does not compile.

## Why a kernel

Every serious multi-agent system ends up hand-building the same checklist: durable
state that survives restarts, stable identity across crashes and respawns, an audit
trail of who did what, authorization for tools and credentials, idempotent recovery,
lifecycle cleanup. Actor frameworks keep agents *alive*; nothing in that stack keeps
them *truthful*.

Atoll's position is that this checklist is not application work — it is an operating
system's work. Agents are the new processes; they need a kernel.

## The minimal kernel: five elements, two planes

```
                 channel (the boundary)
   ┌──────────────────────────────────────────────┐
   │   actor  ──── message ────▶  actor            │   horizontal plane:
   │  (subject)   (into the truth log)  (subject)  │   collaboration = truth
   │     │                                         │
   │     └────── access ────▶  resource            │   vertical plane:
   │        (capability-gated,  (object)           │   reaching data = authority
   │         off-log)                              │
   └──────────────────────────────────────────────┘
```

| Element | What it is |
|---|---|
| **channel** | The boundary atom: one execution domain, one trust boundary, one data boundary. |
| **actor** | A subject: agent, human, or tool. Addressed by identity; alive as an incarnation. |
| **resource** | An object: passive owned data (state, files, secrets), opaque to the kernel. |
| **message** | Subject ↔ subject. Appended to the channel's truth log, delivered from its tail. |
| **access** | Subject ↔ object. Off-log, checked at a gate: caller + resource + operation + capability. |

Remove any element and the system stops working; anything more is domain. Messages
are the IPC of this world; access is its syscall. A runtime with only the horizontal
plane lets agents talk but not act — credentials, state, files, and timers all live
on the vertical plane.

## What the kernel enforces

- **The server is truth.** Every message is appended to a per-channel log (SQLite
  today) before delivery; delivery *is* the log tail. There is no side channel.
- **Writes are welded to identity.** An actor writes through a *pen* minted at birth;
  sender identity is not a field it fills in, so it is not a field it can forge.
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
             sqlite store, schedule/timers, access door)
lib/         stdlib for actor authors (behavior, channelkit, metatool, introspect)
platform/    channel assembly (server-side ChannelHome + daemon-side RunCompute)
app/         HTTP API surface (identity, workspace, channel, daemon, WS)
drivers/     external-world drivers: tools/ (echo, device, kimi, xhs), agents/ (LLM engine providers: claudecode, kimi), gateway/ (human ingress)
registry/    actor class registry (config → running actor)
cmd/         binaries (server, daemon, cli)
archtest/    architecture enforcement tests
```

## Quickstart

```bash
# 1. build
make build          # -> bin/atoll-server, bin/atoll-daemon

# 2. run the server (holds truth for all channels)
bin/atoll-server --db /tmp/atoll-dev/app.db --channel-db-dir /tmp/atoll-dev/channels

# 3. run a daemon (compute host; echo actor needs no external credentials)
#    create a daemon in the UI/CLI to get an api-key, bind it to a channel
bin/atoll-daemon --server "ws://localhost:8080/compute?key=<api-key>&channel=<chID>" \
                 --key <api-key> --actors echo

# 4. tests
make test
```

The web UI lives in a separate repository and is served via `--ui-dist`.

## Writing an actor

An actor implements `Receive` and registers a constructor. The capabilities it gets
(`Caps`) are handed to it at birth — including the pen that welds its identity:

```go
// drivers/tools/hello/hello.go
func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
    // handle the request, write the reply — the pen fills in identity;
    // Sender/ChannelID are not yours to set.
    _, err := a.pen.Write(ctx, responseEnvelope)
    return err
}
```

```go
// drivers/tools/hello/register.go
func init() { registry.Register("hello", construct) }

func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
    return platform.ActorDecl{
        ID:      spec.ID,
        Kind:    actor.KindTool,
        Binding: actor.BindingRuntimeOutbound,
        Factory: func(caps actorcaps.Caps) actorrt.Actor { return NewActor(caps.Pen) },
    }, nil
}
```

## Status

Atoll is **v0.01 — a working minimal kernel, pre-release**. The five elements and
both planes are in place and enforced; the developer shell around them (one-command
setup, coding-agent connectors, scaffolding) is being built next. Known, deliberate
boundaries at this stage: single trust domain per deployment, no read-path ACL yet,
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
