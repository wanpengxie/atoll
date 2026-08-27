'use strict'

// These run the real CLI as a child process against a stand-in adapter, because
// the only thing worth asserting about a forwarder is what actually comes out
// the other end.

const assert = require('node:assert')
const test = require('node:test')
const path = require('node:path')
const { spawn } = require('node:child_process')
const { WebSocketServer, WebSocket } = require('ws')

const BIN = path.join(__dirname, '..', 'cli.js')

// startUpstream stands in for the adapter: it listens, and echoes back a reply
// frame paired to whatever correlation_id it is sent.
function startUpstream () {
  return new Promise((resolve) => {
    const wss = new WebSocketServer({ host: '127.0.0.1', port: 0, path: '/device' })
    const seen = []
    wss.on('connection', (conn) => {
      conn.on('message', (raw) => {
        const frame = JSON.parse(raw.toString())
        seen.push(frame)
        conn.send(JSON.stringify({ correlation_id: frame.correlation_id, ok: true, result: { echoed: frame.cmd } }))
      })
    })
    wss.on('listening', () => {
      const close = () => {
        for (const client of wss.clients) client.terminate()
        wss.close()
      }
      resolve({ wss, seen, close, url: `ws://127.0.0.1:${wss.address().port}/device` })
    })
  })
}

function startBridge (args) {
  const child = spawn(process.execPath, [BIN, ...args], { stdio: ['ignore', 'pipe', 'pipe'] })
  const stderr = []
  child.stderr.on('data', (chunk) => stderr.push(chunk.toString()))
  return { child, stderr, text: () => stderr.join('') }
}

function waitFor (predicate, timeoutMs = 4000) {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    const tick = () => {
      if (predicate()) return resolve()
      if (Date.now() > deadline) return reject(new Error('timed out waiting'))
      setTimeout(tick, 20)
    }
    tick()
  })
}

function freePort () {
  return new Promise((resolve) => {
    const probe = require('node:net').createServer()
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address()
      probe.close(() => resolve(port))
    })
  })
}

test('a frame sent by the plugin reaches the adapter and its reply comes back', async (t) => {
  const upstream = await startUpstream()
  const port = await freePort()
  const bridge = startBridge(['--map', `127.0.0.1:${port}=${upstream.url}`])
  t.after(() => { bridge.child.kill(); upstream.close() })

  await waitFor(() => bridge.text().includes('listening on'))

  const plugin = new WebSocket(`ws://127.0.0.1:${port}/device`)
  const replies = []
  plugin.on('message', (raw) => replies.push(JSON.parse(raw.toString())))
  await new Promise((resolve) => plugin.on('open', resolve))

  plugin.send(JSON.stringify({ correlation_id: 'req-1', cmd: 'search', params: { keyword: 'go' } }))

  await waitFor(() => replies.length > 0)
  assert.deepStrictEqual(replies[0], { correlation_id: 'req-1', ok: true, result: { echoed: 'search' } })
  assert.strictEqual(upstream.seen[0].cmd, 'search')
  assert.deepStrictEqual(upstream.seen[0].params, { keyword: 'go' })
  plugin.close()
})

// A plugin that is quick off the mark can send before the upstream socket has
// finished connecting. Those frames must be carried, not silently dropped —
// a dropped first command looks exactly like a hung adapter.
test('a frame sent before the upstream is up is still delivered', async (t) => {
  const upstream = await startUpstream()
  const port = await freePort()
  const bridge = startBridge(['--map', `127.0.0.1:${port}=${upstream.url}`])
  t.after(() => { bridge.child.kill(); upstream.close() })
  await waitFor(() => bridge.text().includes('listening on'))

  const plugin = new WebSocket(`ws://127.0.0.1:${port}/device`)
  const replies = []
  plugin.on('message', (raw) => replies.push(JSON.parse(raw.toString())))
  plugin.on('open', () => {
    // Sent immediately on open — the bridge's upstream dial has almost
    // certainly not completed yet.
    plugin.send(JSON.stringify({ correlation_id: 'early', cmd: 'snapshot', params: {} }))
  })

  await waitFor(() => replies.length > 0)
  assert.strictEqual(replies[0].correlation_id, 'early')
  plugin.close()
})

// When the adapter goes away the plugin must find out, rather than sit holding
// a socket that leads nowhere.
test('losing the adapter closes the plugin connection', async (t) => {
  const upstream = await startUpstream()
  const port = await freePort()
  const bridge = startBridge(['--map', `127.0.0.1:${port}=${upstream.url}`])
  t.after(() => { bridge.child.kill() })
  await waitFor(() => bridge.text().includes('listening on'))

  const plugin = new WebSocket(`ws://127.0.0.1:${port}/device`)
  let closed = false
  plugin.on('close', () => { closed = true })
  await new Promise((resolve) => plugin.on('open', resolve))
  plugin.send(JSON.stringify({ correlation_id: 'x', cmd: 'ping', params: {} }))
  await waitFor(() => upstream.seen.length > 0)

  upstream.close()
  for (const client of upstream.wss.clients) client.terminate()

  await waitFor(() => closed)
})

// The whole reason the bridge exists is that the adapter is somewhere else. If
// something is already serving locally, that is almost always a local adapter
// the plugin can reach on its own — say so and stop, rather than fight for the
// port or start a forwarder nobody needs.
test('refuses to start when the local port is already taken, and explains why', async () => {
  const upstream = await startUpstream()
  const squatter = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await new Promise((resolve) => squatter.on('listening', resolve))
  const port = squatter.address().port

  const bridge = startBridge(['--map', `127.0.0.1:${port}=${upstream.url}`])
  const code = await new Promise((resolve) => bridge.child.on('exit', resolve))

  assert.strictEqual(code, 3)
  assert.match(bridge.text(), /already in use/)
  assert.match(bridge.text(), /do not need this route/)
  squatter.close()
  upstream.close()
})

// The upstream address is a launch decision. Anything that could be re-pointed
// from outside would be an open proxy, so the flag is the only way in — and a
// non-loopback local bind is refused for the same reason.
test('refuses a wildcard or routable local bind', async () => {
  const upstream = await startUpstream()
  for (const [addr, pattern] of [
    ['0.0.0.0:9', /wildcard/],
    [':9', /host:port/],
    ['10.0.0.1:9', /not loopback/]
  ]) {
    const bridge = startBridge(['--map', `${addr}=${upstream.url}`])
    const code = await new Promise((resolve) => bridge.child.on('exit', resolve))
    assert.strictEqual(code, 2, `${addr} should have been refused`)
    assert.match(bridge.text(), pattern)
  }
  upstream.close()
})

test('refuses to run without a route', async () => {
  const bridge = startBridge([])
  const code = await new Promise((resolve) => bridge.child.on('exit', resolve))
  assert.strictEqual(code, 2)
  assert.match(bridge.text(), /at least one --map is required/)
})

// Two plugins installed means two adapters, two local ports and two upstreams —
// and the pairing between them cannot be derived, only stated. One process
// carries both, and the routes are genuinely independent: a frame on one must
// not reach the other's adapter, and one plugin connecting must not disturb the
// other's live connection.
test('two routes run side by side without crossing', async (t) => {
  const a = await startUpstream()
  const b = await startUpstream()
  const portA = await freePort()
  const portB = await freePort()
  const bridge = startBridge([
    '--map', `127.0.0.1:${portA}=${a.url}`,
    '--map', `127.0.0.1:${portB}=${b.url}`
  ])
  t.after(() => { bridge.child.kill(); a.close(); b.close() })
  await waitFor(() => (bridge.text().match(/listening on/g) || []).length === 2)

  const pluginA = new WebSocket(`ws://127.0.0.1:${portA}/device`)
  const pluginB = new WebSocket(`ws://127.0.0.1:${portB}/device`)
  const repliesA = []
  const repliesB = []
  pluginA.on('message', (raw) => repliesA.push(JSON.parse(raw.toString())))
  pluginB.on('message', (raw) => repliesB.push(JSON.parse(raw.toString())))
  await Promise.all([
    new Promise((resolve) => pluginA.on('open', resolve)),
    new Promise((resolve) => pluginB.on('open', resolve))
  ])

  pluginA.send(JSON.stringify({ correlation_id: 'a-1', cmd: 'search', params: {} }))
  pluginB.send(JSON.stringify({ correlation_id: 'b-1', cmd: 'navigate', params: {} }))
  await waitFor(() => repliesA.length > 0 && repliesB.length > 0)

  assert.strictEqual(repliesA[0].correlation_id, 'a-1')
  assert.strictEqual(repliesB[0].correlation_id, 'b-1')
  assert.deepStrictEqual(a.seen.map((f) => f.cmd), ['search'])
  assert.deepStrictEqual(b.seen.map((f) => f.cmd), ['navigate'])
  assert.strictEqual(pluginA.readyState, WebSocket.OPEN, 'the second plugin displaced the first')
  assert.strictEqual(pluginB.readyState, WebSocket.OPEN)
  pluginA.close()
  pluginB.close()
})

// The same local port cannot forward to two adapters, and that is a mistake
// worth catching at launch rather than as "one of my plugins randomly talks to
// the wrong thing".
test('refuses the same local address twice', async () => {
  const upstream = await startUpstream()
  const bridge = startBridge([
    '--map', `127.0.0.1:9=${upstream.url}`,
    '--map', `127.0.0.1:9=${upstream.url}`
  ])
  const code = await new Promise((resolve) => bridge.child.on('exit', resolve))
  assert.strictEqual(code, 2)
  assert.match(bridge.text(), /more than one --map/)
  upstream.close()
})

// A route that cannot bind must stop the whole bridge. A half-started bridge
// where one plugin's route silently never came up is indistinguishable from a
// hung adapter, which is far harder to diagnose than an error at launch.
test('one unbindable route stops the whole bridge and names it', async () => {
  const upstream = await startUpstream()
  const squatter = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await new Promise((resolve) => squatter.on('listening', resolve))
  const taken = squatter.address().port
  const free = await freePort()

  const bridge = startBridge([
    '--map', `127.0.0.1:${free}=${upstream.url}`,
    '--map', `127.0.0.1:${taken}=${upstream.url}`
  ])
  const code = await new Promise((resolve) => bridge.child.on('exit', resolve))
  assert.strictEqual(code, 3)
  assert.match(bridge.text(), new RegExp(`127\\.0\\.0\\.1:${taken} is already in use`))
  squatter.close()
  upstream.close()
})
