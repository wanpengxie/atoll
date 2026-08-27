#!/usr/bin/env node
'use strict'

// atoll-plugin-bridge — an OPTIONAL tool for one situation: your browser is on
// your laptop, and the atoll plugin adapters that want to drive it are running
// somewhere else. Nobody else needs to run this. If the adapters are on the same
// machine as your browser, the plugins reach them directly and this program has
// no job to do (it will say so and exit).
//
// It is a forwarder and nothing else. It holds no state, understands none of
// the frames it carries, knows nothing about xhs or kimi or any other adapter,
// and cannot be told at runtime where to connect: every route comes from the
// command line, once, at launch. That is deliberate — a forwarder that could be
// re-pointed by something arriving over its own socket would be an open proxy
// wearing a helpful name.
//
//   plugin ──dials──▶ this bridge (127.0.0.1:8090) ──dials──▶ adapter (remote)
//
// The direction matters: the adapter LISTENS and the plugin DIALS IN. So the
// bridge stands in for the adapter locally — the plugin keeps its existing
// localhost configuration and never learns anything moved.
//
// One process carries as many routes as you give it, because you may well have
// two plugins installed. Each route is a PAIR, not a port in a list: the local
// port is fixed by the plugin that dials it, the remote address is wherever
// `listen.set` put that adapter, and neither can be derived from the other. So
// they are named together, and each route runs independently — one adapter
// being unreachable leaves the others carrying traffic.

const http = require('http')
const net = require('net')
const { WebSocketServer, WebSocket } = require('ws')

const USAGE = `atoll-plugin-bridge — forward local browser plugins to remote atoll adapters

  atoll-plugin-bridge --map <local-host:port>=<ws-url> [--map ...]

  --map    Required, repeatable. One route: where this bridge listens for a
           plugin, and where it forwards to. Give one --map per adapter.

             --map 127.0.0.1:8090=ws://100.64.0.7:8090/device   # xhs
             --map 127.0.0.1:8091=ws://100.64.0.7:8091/device   # kimi

           LEFT is the port your plugin already dials — the plugin decides it,
           so it does not change. RIGHT is where that adapter is listening; ask
           the adapter itself with xhs.listen.get / kimi.listen.get and use the
           address it reports. The two sides are unrelated: the ports need not
           match, and nothing here can guess one from the other.

           The local side must be a loopback address, because this endpoint is
           keyless — exactly like the adapter's.

  --quiet  Only report errors.
  --help   Print this.
`

function parseArgs (argv) {
  const out = { maps: [], quiet: false }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    switch (arg) {
      case '--help':
      case '-h':
        out.help = true
        break
      case '--quiet':
        out.quiet = true
        break
      case '--map': {
        const value = argv[++i]
        if (value === undefined || value.startsWith('--')) {
          throw new Error(`${arg} needs a value`)
        }
        out.maps.push(value)
        break
      }
      default:
        throw new Error(`unknown argument ${arg}`)
    }
  }
  return out
}

// parseMap splits one route. The separator is the FIRST '=', because the
// upstream URL on the right may contain one and the local address never does.
function parseMap (spec) {
  const at = spec.indexOf('=')
  if (at <= 0) {
    throw new Error(`--map must be <local-host:port>=<ws-url>, got ${spec}`)
  }
  return {
    listen: checkListen(spec.slice(0, at)),
    upstream: checkUpstream(spec.slice(at + 1)),
    spec
  }
}

// checkListen keeps the local side to the same shape the adapter accepts, and
// refuses a wildcard for the same reason the adapter does: this endpoint has no
// key, so "listen everywhere" would hand your browser to every network this
// machine happens to be on.
function checkListen (addr) {
  const at = addr.lastIndexOf(':')
  if (at <= 0) throw new Error(`the local side of --map must be host:port, got ${addr}`)
  const host = addr.slice(0, at).replace(/^\[|\]$/g, '')
  const port = Number(addr.slice(at + 1))
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`the local side of --map is not a port: ${addr}`)
  }
  if (host === '' || host === '0.0.0.0' || host === '::') {
    throw new Error(`--map local side ${addr} is a wildcard; this endpoint is keyless, so name the loopback address explicitly`)
  }
  const family = net.isIP(host)
  const loopback = host === 'localhost' ||
    (family === 4 && host.startsWith('127.')) ||
    (family === 6 && (host === '::1' || host === '0:0:0:0:0:0:0:1'))
  if (!loopback) {
    throw new Error(`--map local side ${addr} is not loopback; the bridge serves the browser on THIS machine, and exposing it further would let anyone who can reach it drive your browser`)
  }
  return { host, port, addr }
}

function checkUpstream (raw) {
  let url
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`the upstream side of --map is not a URL: ${raw}`)
  }
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`the upstream side of --map must be ws:// or wss://, got ${url.protocol}`)
  }
  return url.toString()
}

// startRoute brings up one local endpoint forwarding to one upstream. Each
// route owns its own single-connection slot: two plugins are two conversations,
// and a new xhs connection has no business displacing a live kimi one.
function startRoute (route, log, onFatal, onListening) {
  const server = http.createServer((req, res) => {
    res.writeHead(426, { 'content-type': 'text/plain' })
    res.end('this endpoint speaks WebSocket only\n')
  })

  // Same origin rule as the adapter, for the same reason: loopback keeps other
  // MACHINES out, but a web page open in your own browser is same-machine and
  // could otherwise open a cross-origin socket here and drive the adapter.
  const wss = new WebSocketServer({
    server,
    verifyClient: ({ origin }) => !origin || origin.startsWith('chrome-extension://')
  })

  let current = null

  wss.on('connection', (plugin) => {
    if (current) {
      // One plugin per route, mirroring the adapter, which also keeps a single
      // connection and lets the newest one win.
      log(`${route.listen.addr}: a new plugin connected; dropping the previous one`)
      current.plugin.close()
    }

    const up = new WebSocket(route.upstream)
    const pair = { plugin, up }
    current = pair

    // Frames that arrive before the upstream socket is open would otherwise be
    // dropped on the floor. There is no reordering here: they go out in the
    // order they arrived, once there is somewhere to send them.
    const backlog = []
    const closePair = (why) => {
      if (current === pair) current = null
      if (why) log(`${route.listen.addr}: ${why}`)
      try { plugin.close() } catch {}
      try { up.close() } catch {}
    }

    up.on('open', () => {
      log(`${route.listen.addr}: plugin ⇄ ${route.upstream}`)
      for (const frame of backlog.splice(0)) up.send(frame)
    })
    up.on('message', (data, isBinary) => {
      if (plugin.readyState === WebSocket.OPEN) plugin.send(data, { binary: isBinary })
    })
    up.on('error', (err) => closePair(`upstream error: ${err.message}`))
    up.on('close', () => closePair('upstream closed'))

    plugin.on('message', (data, isBinary) => {
      if (up.readyState === WebSocket.OPEN) up.send(data, { binary: isBinary })
      else if (up.readyState === WebSocket.CONNECTING) backlog.push(data)
    })
    plugin.on('error', (err) => closePair(`plugin error: ${err.message}`))
    plugin.on('close', () => closePair('plugin disconnected'))
  })

  // ws forwards the http server's errors onto the WebSocketServer, so the
  // handler has to sit on both — an unhandled 'error' there would surface as a
  // raw stack trace instead of the sentence that tells the operator what to do.
  const onError = (err) => onFatal(route, err)
  server.on('error', onError)
  wss.on('error', onError)

  server.listen(route.listen.port, route.listen.host, () => {
    log(`listening on ${route.listen.addr}/device → ${route.upstream}`)
    onListening()
  })

  return { server, wss }
}

function main () {
  let args
  try {
    args = parseArgs(process.argv.slice(2))
  } catch (err) {
    process.stderr.write(`${err.message}\n\n${USAGE}`)
    process.exit(2)
  }
  if (args.help) {
    process.stdout.write(USAGE)
    return
  }
  if (args.maps.length === 0) {
    process.stderr.write(`at least one --map is required\n\n${USAGE}`)
    process.exit(2)
  }

  let routes
  try {
    routes = args.maps.map(parseMap)
  } catch (err) {
    process.stderr.write(`${err.message}\n`)
    process.exit(2)
  }
  const seen = new Set()
  for (const route of routes) {
    if (seen.has(route.listen.addr)) {
      process.stderr.write(`${route.listen.addr} appears in more than one --map; each local address can forward to exactly one adapter\n`)
      process.exit(2)
    }
    seen.add(route.listen.addr)
  }

  const log = args.quiet ? () => {} : (...m) => process.stderr.write(`${m.join(' ')}\n`)

  // A route that cannot bind stops the whole bridge rather than leaving a
  // half-started one: a plugin whose route silently never came up looks exactly
  // like a hung adapter, and that is a much worse afternoon than an error at
  // launch saying which --map to drop.
  const onFatal = (route, err) => {
    if (err.code === 'EADDRINUSE') {
      // Almost always this means an adapter is already running on this machine
      // — in which case that plugin can reach it directly and this route is not
      // wanted. Say that, rather than a bare errno.
      process.stderr.write(
        `${route.listen.addr} is already in use.\n` +
        'If an atoll adapter is already running here, your plugin can reach it directly and you do not need this route.\n' +
        `Drop --map ${route.spec}, or give it a free local port and point the plugin at that instead.\n`)
      process.exit(3)
    }
    process.stderr.write(`${route.listen.addr}: ${err.message}\n`)
    process.exit(1)
  }

  // The "ready" line waits for every route to actually bind, so it can never
  // appear above an error saying one of them did not.
  let pending = routes.length
  const onListening = () => {
    if (--pending === 0) {
      log(`bridge: ${routes.length} route${routes.length === 1 ? '' : 's'} up; point your plugins at the local addresses above, nothing else needs to change`)
    }
  }
  const running = routes.map((route) => startRoute(route, log, onFatal, onListening))

  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => {
      log('bridge: stopping')
      for (const { server, wss } of running) {
        wss.close()
        server.close()
      }
      process.exit(0)
    })
  }
}

main()
