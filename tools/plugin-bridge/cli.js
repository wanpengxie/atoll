#!/usr/bin/env node
'use strict'

// atoll-plugin-bridge — an OPTIONAL tool for one situation: your browser is on
// your laptop, and the atoll plugin adapter that wants to drive it is running
// somewhere else. Nobody else needs to run this. If the adapter is on the same
// machine as your browser, the plugin reaches it directly and this program has
// no job to do (it will say so and exit).
//
// It is a forwarder and nothing else. It holds no state, understands none of
// the frames it carries, and cannot be told at runtime where to connect: the
// upstream address comes from the command line, once, at launch. That is
// deliberate — a forwarder that could be re-pointed by something arriving over
// its own socket would be an open proxy wearing a helpful name.
//
//   plugin ──dials──▶ this bridge (127.0.0.1:8090) ──dials──▶ adapter (remote)
//
// The direction matters: the adapter LISTENS and the plugin DIALS IN. So the
// bridge stands in for the adapter locally — the plugin keeps its existing
// localhost configuration and never learns anything moved.

const http = require('http')
const net = require('net')
const { WebSocketServer, WebSocket } = require('ws')

const USAGE = `atoll-plugin-bridge — forward a local browser plugin to a remote atoll adapter

  atoll-plugin-bridge --upstream <ws-url> [--listen <host:port>]

  --upstream  Required. Where the adapter is listening, as a WebSocket URL,
              e.g. ws://100.64.0.7:8090/device
              Get it from the adapter itself: send xhs.listen.get (or
              kimi.listen.get) and use the address it reports.
  --listen    Where this bridge listens for the plugin. Default 127.0.0.1:8090
              (xhs); use 127.0.0.1:8091 for kimi. Must be a loopback address:
              this endpoint is keyless, exactly like the adapter's.
  --quiet     Only report errors.
  --help      Print this.
`

function parseArgs (argv) {
  const out = { listen: '127.0.0.1:8090', quiet: false }
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
      case '--upstream':
      case '--listen': {
        const value = argv[++i]
        if (value === undefined || value.startsWith('--')) {
          throw new Error(`${arg} needs a value`)
        }
        out[arg.slice(2)] = value
        break
      }
      default:
        throw new Error(`unknown argument ${arg}`)
    }
  }
  return out
}

// splitHostPort keeps the local listen address to the same shape the adapter
// accepts, and refuses a wildcard for the same reason the adapter does: this
// endpoint has no key, so "listen everywhere" would hand your browser to every
// network this machine happens to be on.
function splitHostPort (addr) {
  const at = addr.lastIndexOf(':')
  if (at <= 0) throw new Error(`--listen must be host:port, got ${addr}`)
  const host = addr.slice(0, at).replace(/^\[|\]$/g, '')
  const port = Number(addr.slice(at + 1))
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`--listen port is not a port: ${addr}`)
  }
  if (host === '' || host === '0.0.0.0' || host === '::') {
    throw new Error(`--listen ${addr} is a wildcard; this endpoint is keyless, so name the loopback address explicitly`)
  }
  const family = net.isIP(host)
  const loopback = host === 'localhost' ||
    (family === 4 && host.startsWith('127.')) ||
    (family === 6 && (host === '::1' || host === '0:0:0:0:0:0:0:1'))
  if (!loopback) {
    throw new Error(`--listen ${addr} is not loopback; the bridge serves the browser on THIS machine, and exposing it further would let anyone who can reach it drive your browser`)
  }
  return { host, port }
}

function checkUpstream (raw) {
  let url
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`--upstream is not a URL: ${raw}`)
  }
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`--upstream must be ws:// or wss://, got ${url.protocol}`)
  }
  return url.toString()
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
  if (!args.upstream) {
    process.stderr.write(`--upstream is required\n\n${USAGE}`)
    process.exit(2)
  }

  let upstream, listen
  try {
    upstream = checkUpstream(args.upstream)
    listen = splitHostPort(args.listen)
  } catch (err) {
    process.stderr.write(`${err.message}\n`)
    process.exit(2)
  }

  const log = args.quiet ? () => {} : (...m) => process.stderr.write(`${m.join(' ')}\n`)

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
      // One plugin, one bridge — mirrors the adapter, which also keeps a single
      // connection and lets the newest one win.
      log('bridge: a new plugin connected; dropping the previous one')
      current.plugin.close()
    }

    const up = new WebSocket(upstream)
    const pair = { plugin, up }
    current = pair

    // Frames that arrive before the upstream socket is open would otherwise be
    // dropped on the floor. There is no reordering here: they go out in the
    // order they arrived, once there is somewhere to send them.
    const backlog = []
    const closePair = (why) => {
      if (current === pair) current = null
      if (why) log(`bridge: ${why}`)
      try { plugin.close() } catch {}
      try { up.close() } catch {}
    }

    up.on('open', () => {
      log(`bridge: plugin ⇄ ${upstream}`)
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
  const onError = (err) => {
    if (err.code === 'EADDRINUSE') {
      // Almost always this means an adapter is already running on this machine
      // — in which case the plugin can reach it directly and the bridge is not
      // wanted. Say that, rather than a bare errno.
      process.stderr.write(
        `${args.listen} is already in use.\n` +
        'If an atoll adapter is already running here, your plugin can reach it directly and you do not need this bridge.\n' +
        'Otherwise pass --listen with a free port and point the plugin at it.\n')
      process.exit(3)
    }
    process.stderr.write(`bridge: ${err.message}\n`)
    process.exit(1)
  }
  server.on('error', onError)
  wss.on('error', onError)

  server.listen(listen.port, listen.host, () => {
    log(`bridge: listening on ${args.listen}/device, forwarding to ${upstream}`)
    log('bridge: point your browser plugin at this address; nothing else needs to change')
  })

  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => {
      log('bridge: stopping')
      wss.close()
      server.close(() => process.exit(0))
    })
  }
}

main()
