package kimi

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/lib/introspect"
)

const ActorDescription = "Drive the user's Kimi WebBridge chrome extension — browser navigation, screenshots, snapshots, clicks, fills, and JavaScript evaluation on the user's real browser session."

const ActorSkillDoc = "" +
	"# kimi (Kimi WebBridge)\n" +
	"\n" +
	"Drives the user's real browser via the Kimi WebBridge Chrome extension.\n" +
	"The extension connects to the local daemon over WebSocket; the daemon\n" +
	"bridges envelope ↔ extension transparently.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"One request type: `kimi.command`.\n" +
	"\n" +
	"Payload:\n" +
	"```json\n" +
	"{\"action\":\"navigate\",\"args\":{\"url\":\"https://example.com\",\"newTab\":true},\"session\":\"kimi\"}\n" +
	"```\n" +
	"\n" +
	"## Available actions\n" +
	"\n" +
	"- `navigate` — open a URL (optionally in new tab)\n" +
	"- `find_tab` — find an existing tab by URL pattern\n" +
	"- `snapshot` — capture the current page DOM snapshot\n" +
	"- `click` — click an element by selector\n" +
	"- `fill` — fill a form field\n" +
	"- `evaluate` — run JavaScript in the page context\n" +
	"- `screenshot` — capture a screenshot (PNG)\n" +
	"- `network` — intercept network requests\n" +
	"- `upload` — upload a file to a file input\n" +
	"- `save_as_pdf` — save the current page as PDF\n" +
	"- `list_tabs` — list open browser tabs\n" +
	"- `close_tab` — close a tab\n" +
	"- `close_session` — close the browser session\n" +
	"\n" +
	"## Common errors\n" +
	"\n" +
	"- `extension_disconnected` — extension not connected; install or reload\n" +
	"- `tool_failed` — extension ran the command but it failed; check detail\n" +
	"- `payload_decode_failed` — malformed payload\n"

// TypeMetadata documents the kimi types in the introspect contract shape.
var TypeMetadata = map[string]introspect.TypeMeta{
	TypeCommand: {
		Description:    "Forward a single Kimi WebBridge command to the user's chrome extension.",
		AllowedKinds:   []string{"request"},
		MaxPendingMs:   DefaultMaxPendingMs,
		PayloadExample: json.RawMessage(`{"action":"snapshot","args":{},"session":"kimi"}`),
	},
}
