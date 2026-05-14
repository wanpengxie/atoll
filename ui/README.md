# coagent UI

Vite + vanilla-JS SPA for the coagent M1.5 demo console.

## Layout

```
ui/
├── index.html          # vite root entry
├── src/
│   ├── main.js         # SPA bootstrap: auth, workspace/channel, messages
│   ├── api.js          # fetch wrapper (cookie-auth, JSON)
│   ├── ws.js           # native WebSocket client for /ws
│   └── styles.css
├── public/
│   └── favicon.svg     # static assets copied verbatim to dist/
├── package.json
└── vite.config.js
```

## Development

```bash
# from repo root
pnpm install
pnpm --filter ui dev
```

Vite dev proxies `/api/*`, `/healthz`, and `/ws` to
`http://localhost:8080` by default. Override with
`VITE_SERVER_URL=https://stage.example pnpm --filter ui dev`.

## Production build

```bash
pnpm --filter ui build
# → ui/dist/{index.html, assets/*, favicon.svg}
```

`cmd/server` will serve `ui/dist/` as the static asset root in a future
milestone; today the build artifact is consumed as a tarball.

## Server API surface

The SPA targets the Go gateway routes documented in
`server/gateway/handlers.go`:

| Action            | Method + path                                  |
| ----------------- | ---------------------------------------------- |
| Issue email code  | `POST /api/identity/verification/issue`        |
| Register          | `POST /api/identity/register`                  |
| Login             | `POST /api/identity/login`                     |
| Logout            | `POST /api/identity/logout`                    |
| Me                | `GET  /api/identity/me`                        |
| List workspaces   | `GET  /api/workspaces`                         |
| Create workspace  | `POST /api/workspaces`                         |
| List channels     | `GET  /api/workspaces/:wsID/channels`          |
| Create channel    | `POST /api/workspaces/:wsID/channels`          |
| List messages     | `GET  /api/channels/:chID/messages?after=&limit=` |
| Write message     | `POST /api/channels/:chID/messages`            |
| Live updates      | `GET  /ws` (native WebSocket; subscribe frames) |

Auth is by cookie (`coagent_session`, `HttpOnly`, `SameSite=Lax`) —
all fetches set `credentials: 'include'`.

## Wire protocol — WS `/ws`

```
client → server   {"type":"subscribe",   "channel_id":"…"}
client → server   {"type":"unsubscribe", "channel_id":"…"}
server → client   {"type":"message", "channel_id":"…",
                   "seq": <int>, "envelope": { … kernel/message.Envelope … }}
```

The legacy `socket.io` transport is gone — no `cdn.socket.io` script
is loaded and no `socket.io` traffic is visible in dev tools.
