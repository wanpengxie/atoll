package kimi

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

// describe.go is the actor.describe self-answer catalog — discovery is the
// actor answering live (no external catalog). kimi exposes a SINGLE type,
// kimi.command, whose 13 browser-primitive actions are enumerated in the type's
// notes (the closed verb set lives in the payload, not the type set).

const actorDescription = "Kimi WebBridge adapter: drive the user's real browser through a connected Chrome extension — navigate, snapshot, click, fill, evaluate JS, screenshot, and more. Send it kimi.command requests carrying an action + args; it forwards to the device and returns business results."

const actorSkillDoc = "" +
	"# kimi (Kimi WebBridge)\n" +
	"\n" +
	"Device-backed adapter that drives the user's real browser via the Kimi\n" +
	"WebBridge Chrome extension; one extension == one device.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"One request type: `kimi.command`, payload `{action, args}`. `action` is one\n" +
	"of 13 browser primitives; `args` passes through to the extension verbatim.\n" +
	"\n" +
	"```json\n" +
	"{\"action\":\"navigate\",\"args\":{\"url\":\"https://example.com\"}}\n" +
	"```\n" +
	"\n" +
	"## Actions\n" +
	"\n" +
	"- `navigate` — open a URL (optionally in a new tab)\n" +
	"- `find_tab` — find an existing tab by URL pattern\n" +
	"- `snapshot` — capture the current page DOM snapshot\n" +
	"- `click` — click an element by selector\n" +
	"- `fill` — fill a form field\n" +
	"- `evaluate` — run JavaScript in the page context\n" +
	"- `screenshot` — capture a screenshot; result is a LOCAL file path (the\n" +
	"  device writes the PNG to disk — bytes do not cross the wire)\n" +
	"- `network` — inspect/intercept network requests\n" +
	"- `upload` — upload a file to a file input\n" +
	"- `save_as_pdf` — save the current page as PDF; result is a LOCAL file path\n" +
	"  (the device writes the PDF to disk — bytes do not cross the wire)\n" +
	"- `list_tabs` — list open browser tabs\n" +
	"- `close_tab` — close a tab\n" +
	"- `close_session` — close the browser session\n" +
	"\n" +
	"## Errors\n" +
	"\n" +
	"- `device_offline` — no extension is connected; retry once a device attaches.\n" +
	"- `invalid_action` — the action is not one of the 13 supported primitives.\n" +
	"- `timeout` — the device did not reply within ~60s.\n" +
	"\n" +
	"## Describe surface\n" +
	"\n" +
	"- `actor.describe` — returns the actor id, this skill doc, and the single\n" +
	"  kimi.command type entry.\n"

// requestKinds is the conventional allowed-kinds value for a request type.
var requestKinds = []string{string(message.KindRequest)}

// describeCatalog builds the full Describe self-answer for this actor id.
func describeCatalog(actorID string) introspect.Describe {
	return introspect.Describe{
		ActorID:     actorID,
		Description: actorDescription,
		SkillDoc:    actorSkillDoc,
		Types: map[string]introspect.TypeMeta{
			TypeCommand: {
				Description:    "Forward one Kimi WebBridge browser command to the user's Chrome extension. The device verb is the payload's `action` (one of 13 primitives); `args` is forwarded verbatim.",
				AllowedKinds:   requestKinds,
				MaxPendingMs:   commandDeadline.Milliseconds(),
				PayloadExample: json.RawMessage(`{"action":"navigate","args":{"url":"https://example.com"}}`),
				PayloadFields: []introspect.FieldDoc{
					{Name: "action", Required: true, Description: "Browser primitive: navigate / find_tab / snapshot / click / fill / evaluate / screenshot / network / upload / save_as_pdf / list_tabs / close_tab / close_session."},
					{Name: "args", Description: "Action-specific arguments, forwarded to the extension verbatim.", Example: map[string]any{"url": "https://example.com"}},
				},
				ErrorCodes: []introspect.ErrorDoc{
					{Code: "device_offline", Description: "No extension connected.", Recovery: "Attach a device and retry."},
					{Code: "invalid_action", Description: "Action not in the 13-primitive set.", Recovery: "Use a supported action."},
					{Code: "timeout", Description: "Device did not reply within ~60s.", Recovery: "Retry; check the extension."},
				},
				Notes: "Actions: navigate, find_tab, snapshot, click, fill, evaluate, screenshot, network, upload, save_as_pdf, list_tabs, close_tab, close_session. screenshot and save_as_pdf return a LOCAL file path (device writes to disk; bytes do not cross the wire).",
			},
		},
	}
}
