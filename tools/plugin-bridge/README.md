# @atoll/plugin-bridge

An **optional** end-side forwarder. You only need it in one situation:

> Your browser is on your own machine, and the atoll adapters that want to
> drive it (`tool:xhs`, `tool:kimi`, …) are running somewhere else.

If the adapter runs on the same machine as your browser — the default — your
plugin reaches it directly and this program has no job to do. Nobody else in
the channel needs to run it, and nothing depends on it being there.

## How the pieces sit

The adapter **listens**; the browser plugin **dials in**. So the bridge stands
in for the adapter locally: your plugin keeps pointing at localhost and never
learns anything moved.

```
browser plugin ──dials──▶ bridge (127.0.0.1:8090) ──dials──▶ adapter (elsewhere)
```

## Use it

Two steps. First, move the adapter's endpoint off loopback so your machine can
reach it — the adapter answers this itself:

```
xhs.listen.set  {"listen_addr": "100.64.0.7:8090"}
xhs.listen.get  {}      → tells you where it is now and whether a plugin is attached
```

(`kimi.listen.set` / `kimi.listen.get` for the kimi adapter, default port 8091.)

Then run the bridge on your machine:

```
npx @atoll/plugin-bridge --map 127.0.0.1:8090=ws://100.64.0.7:8090/device
```

Leave your plugin configured for `127.0.0.1:8090` as before.

### One `--map` per plugin

Two plugins installed means two adapters, so give the bridge two routes. One
process carries both:

```
npx @atoll/plugin-bridge \
  --map 127.0.0.1:8090=ws://100.64.0.7:8090/device \
  --map 127.0.0.1:8091=ws://100.64.0.7:8091/device
```

A route is a **pair**, not a port in a list, and it has to be spelled out
because neither half can be derived from the other:

- the **left** side is fixed by the plugin — that is the address it already
  dials, and it does not change;
- the **right** side is wherever you put that adapter with `listen.set`.

The ports need not match, and the bridge knows nothing about xhs or kimi, so it
cannot guess. Routes are independent: one adapter being unreachable leaves the
others carrying traffic, and each keeps its own connection — a plugin connecting
on one route never disturbs another.

```
--map    Required, repeatable. <local-host:port>=<ws-url>. The local side must
         be loopback, because this endpoint is keyless just like the adapter's.
--quiet  Only report errors.
```

Exit codes: `2` bad arguments, `3` a local port is already taken (usually
because an adapter is already running here, in which case that plugin can reach
it directly and you can drop that route). A route that cannot bind stops the
whole bridge rather than leaving one silently missing.

## What it does and does not do

It forwards frames and nothing else. It keeps no state, does not read the
frames it carries, and **cannot be re-pointed at runtime** — the upstream
address comes from the command line, once, at launch. That is deliberate: a
forwarder that could be told over its own socket where to connect would be an
open proxy wearing a helpful name.

One plugin at a time, matching the adapter, and the newest connection wins.
When the adapter goes away the plugin's connection is closed rather than left
holding a socket that leads nowhere.

## Know what you are exposing

The adapter's device endpoint is **keyless**. Whatever can reach the address you
set with `listen.set` can drive your browser through the plugin. On loopback
that means "a process on that machine"; on a tailnet it means "anything on the
tailnet". Choose an address whose reachable set you actually intend — a
wildcard bind (`0.0.0.0`, `::`) is refused by both the adapter and this bridge
for exactly that reason.

## Develop

```
npm install
npm test
```

`drivers/tools/xhs/bridge_test.go` in the atoll repo runs this bridge against a
real adapter end to end; it skips if `npm install` has not been run here.
