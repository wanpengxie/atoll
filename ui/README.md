# coagent UI

Vite + vanilla-JS SPA for the coagent demo console. M1.6-T5 upgraded
the renderer to the **v4 Layer 3 chat-as-UI** contract (visibility/kind
matrix, correlation_id story grouping, parent_id thread folding, inline
media, browser notifications, unread badges, error reason mapping).

## Layout

```
ui/
├── index.html          # vite root entry
├── src/
│   ├── main.js         # SPA wiring: auth, ws, composer, mount points
│   ├── api.js          # fetch wrapper (cookie-auth, JSON)
│   ├── ws.js           # native WebSocket client for /ws
│   ├── protocol.js     # v4 envelope closed enums + predicates
│   ├── threading.js    # correlation_id + parent_id story/thread grouping
│   ├── renderer.js     # envelope → DOM (bubbles / folds / threads)
│   ├── media.js        # doc_refs inline image/video/markdown/file render
│   ├── unread.js       # cursor (localStorage) + sidebar badge calc
│   ├── notify.js       # browser Notification API (mention / response)
│   ├── errors.js       # L1 §10.3 reason → composer / system-event map
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
`http://localhost:8832` by default. Override with
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
| Get placement     | `GET  /api/placements/:chID`                   |
| Issue device sess | `POST /api/channels/:chID/devices`             |
| Revoke device     | `DELETE /api/devices/:sid`                     |

Auth is by cookie (`coagent_session`, `HttpOnly`, `SameSite=Lax`) —
all fetches set `credentials: 'include'`.

## Bind Chrome extension (T148 / M1.6-T6)

The chat header has a "Bind Chrome extension" button that hands a
fresh device session token to the coagent xhs-extension via
`chrome.runtime.sendMessage`. Pre-reqs:

1. Install the unpacked extension from
   `adapters/device/xhs/extension/app/chrome-extension` (or the Chrome
   Web Store build for prod).
2. Copy its id from `chrome://extensions` into `ui/.env.local`:

   ```
   VITE_COAGENT_EXTENSION_ID=ngghjmpccpgmfgblbifmlmjnnpfknhka
   ```

3. The extension's manifest `externally_connectable.matches` must include
   the origin you serve the UI from. Set `COAGENT_WEB_ORIGINS` at
   extension build time (comma-separated Chrome match patterns) — e.g.
   `COAGENT_WEB_ORIGINS=https://*.coagent.dev/*,http://localhost:*/*`.

If the button is disabled the status line below it explains why
(`no_extension_id`, `extension_not_installed`, etc.).

## Wire protocol — WS `/ws`

```
client → server   {"type":"subscribe",   "channel_id":"…"}
client → server   {"type":"unsubscribe", "channel_id":"…"}
server → client   {"type":"message", "channel_id":"…",
                   "seq": <int>, "envelope": { … kernel/message.Envelope … }}
```

The legacy `socket.io` transport is gone — no `cdn.socket.io` script
is loaded and no `socket.io` traffic is visible in dev tools.

## Render pipeline — v4 Layer 3 contract

1. `ws.js` / `api.js` produce **envelope-shaped** message rows. Every
   envelope carries the L0 §2.1 17 fields verbatim — UI does not drop or
   reinterpret any field.
2. `main.js#normalizeStoredMessage` projects each row into the
   renderer-friendly shape (defaulting missing fields).
3. `threading.groupTimeline` builds the entries:
   - **story group** (level 1): all envelopes sharing one
     `correlation_id` collapse into one logical block.
   - **request-response thread** (level 2): each `kind=request` pairs
     to at most one terminal `kind=response` via `parent_id`.
   - **system fold**: `visibility=system` messages route to the system
     events drawer (default collapsed); intermediate `agent.text +
     visibility=system` collapses under the public reply.
4. `renderer.buildEntryNode` emits the DOM. Bubble shapes branch on
   `sender.kind`; alignment branches on self-vs-other; mention border
   from `audience` membership; inactive tag from `actor_registry.
   deregistered_at`.
5. `media.appendInlineMedia` walks `doc_refs` and inlines image / video
   / markdown / pdf (proxy URL = `/api/channels/:id/files/:path`).
6. `unread` tracks `last_consumed_seq` per channel in `localStorage`;
   badge calc obeys the §7.1 ACL (system never counts; private only when
   sender = self).
7. `notify.classifyNotification` decides whether an incoming envelope
   warrants a browser notification — `kind=request` to self, `kind=response`
   whose parent was sent by self, `kind=event` audience-containing self.
   Permission is requested on first login.
8. `errors.classifyReason` maps L1 §10.3 reason strings to one of 5
   classes (user_input / identity / protocol_system / failed_terminal /
   install_system) and drives where the message appears (composer red
   bar vs system events drawer vs thread status row).

## Manual e2e checklist — v4 Layer 3 acceptance

Run a fresh server+daemon (`make build && bin/coagent-server` +
`bin/coagent-daemon`) and walk through:

| # | Step | Expected |
|---|------|----------|
| 1 | Open `/`, register → login | Sidebar shows workspaces; chat pane shows "Select a channel" |
| 2 | Create workspace + channel (type=`group`) | Channel appears, badge 0 |
| 3 | Send `human.text` message in composer | Right-aligned green bubble |
| 4 | Trigger an agent reply (mock bridge) | Left-aligned bubble, agent avatar circular |
| 5 | Trigger an agent turn with `visibility=system` intermediate steps | "▸ 思考过程 (N 步)" fold under the public reply; click to expand |
| 6 | Send `xhs.publish` request (via xhs CLI or `coagent ask`) | "▸ 工具调用 (1: xhs.publish)" fold; status shows ⏳ 工具处理中, then ✓ 已响应 |
| 7 | Publish a message with `audience=[<self-actor>]` | Border-left red @ highlight; sidebar badge +1 if channel not active |
| 8 | Background tab + receive @ mention | Browser notification fires (after permission grant) |
| 9 | Attach `.png` via doc_refs | Inline image preview, click to open original |
| 10 | Receive a `system.event` | Bottom-of-chat "显示系统事件" toggle appears; expanding shows the event |
| 11 | Receive a `failed terminal` (status=failed) | Thread status row shows "✗ 失败：<中文 reason>" |
| 12 | Deregister an actor + send a new message | Sender name shows "(inactive)" grey tag |

Acceptance bullets A1-A7 from `.dalek/pm/m1.6-tickets.md` (T5 ticket)
map onto this checklist 1:1.

## Testing

Pure-function modules (`protocol.js` / `threading.js` / `unread.js` /
`errors.js` / `notify.js#classifyNotification`) are written to be
unit-testable. A vitest + jsdom test target is **not yet wired** — the
follow-up is to add `vitest` + `jsdom` as dev deps and a `pnpm --filter
ui test` script. The modules already export every function the tests
need.
