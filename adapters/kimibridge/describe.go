package kimibridge

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// ActorDescription is the deprecated direct actor positioning retained
// for schema consumers.
const ActorDescription = "Drive the user's real browser through Kimi WebBridge (local daemon + Chrome extension). Use to navigate, click, fill, screenshot, evaluate JS, and read accessibility snapshots on live web pages."

// ActorSkillDoc is the deprecated direct actor usage guide retained for
// schema consumers.
const ActorSkillDoc = "" +
	"# kimi-webbridge\n" +
	"\n" +
	"Drives a real Chromium browser through a local daemon at `http://127.0.0.1:10086`. " +
	"All tools share a `session` argument (default: `kimi`) that scopes tabs together — " +
	"close every session at task end via `kimibridge.close_session`.\n" +
	"\n" +
	"## Typical workflow\n" +
	"\n" +
	"1. `kimibridge.navigate {url, newTab:true}` — open the target page in a fresh tab.\n" +
	"2. `kimibridge.snapshot {}` — read the accessibility tree (text with `@e<n>` refs).\n" +
	"3. Inspect the snapshot to locate elements. Drive them with:\n" +
	"   - `kimibridge.click {selector:\"@e42\"}` or CSS selectors.\n" +
	"   - `kimibridge.fill {selector:\"@e42\", value:\"...\"}` — handles `<input>`, `<textarea>`, and contenteditable (ProseMirror/Lexical/Slate).\n" +
	"4. For complex DOM reads use `kimibridge.evaluate {code}` (supports async/await).\n" +
	"5. `kimibridge.screenshot {format:\"png\"}` or `kimibridge.save_as_pdf {...}` to capture state.\n" +
	"6. `kimibridge.close_session {}` to release tabs.\n" +
	"\n" +
	"## Reusing an open tab\n" +
	"\n" +
	"Pass `kimibridge.find_tab {url:\"<domain or url>\", active:true}` when the user says " +
	"\"in my current X page\". Without `active:true` the leftmost matching tab wins.\n" +
	"\n" +
	"## Reading page content\n" +
	"\n" +
	"`snapshot` returns the accessibility tree as text, not HTML. Elements appear like " +
	"`button \"Submit\" @e42` — the `@eNNN` ref is what `click` / `fill` accept. Prefer " +
	"refs over CSS selectors when both work (they survive DOM churn between calls).\n" +
	"\n" +
	"## Files\n" +
	"\n" +
	"`screenshot` / `save_as_pdf` write to disk and return `{path, sizeBytes}`. " +
	"Read the file via the agent's `Read` tool (or doc_refs once L4 lands). " +
	"Pass an explicit `path` arg to control the destination; omit to use an OS temp file.\n" +
	"\n" +
	"## Common error surface\n" +
	"\n" +
	"- `daemon_call_failed` — local daemon unreachable. Surfaced as `reason=receiver_unavailable`.\n" +
	"- `tool_failed` — daemon ran the tool but it failed (no tab found, selector miss, no extension). " +
	"Surfaced as `reason=receiver_internal_error`; check `detail` for daemon-side message.\n" +
	"- `payload_decode_failed` — args JSON not valid for the tool.\n"

// rrKindsField is reused across every R/R type for AllowedKinds.
// Keep in sync with DeclarationTypeDeclarations.
//
// The slice itself is constructed once and shared because TypeDeclaration
// is treated as read-only by the framework (Manager.Install copies what
// it needs). Tests that compare slices may want to clone first.
//
// (No event-only kinds for kimibridge v1.)
//
// nolint: unused // wired through DeclarationTypeDeclarations below.

// typeMeta carries the actor-CLI describe_type convention metadata for
// each type. Stored as a map so DeclarationTypeDeclarations can merge it
// into the per-type TypeDeclaration. Unset types fall back to bare
// AllowedKinds + TerminalConvention.
var typeMeta = map[string]adapter.TypeDeclaration{
	TypeNavigate: {
		Description: "Open a URL in the browser. Use `newTab:true` on the first navigation of a task; reuse an existing tab via find_tab afterwards.",
		PayloadExample: json.RawMessage(
			`{"args":{"url":"https://example.com","newTab":true,"group_title":"research"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.url", Required: true, Description: "Absolute URL to navigate to.", Example: "https://example.com"},
			{Name: "args.newTab", Description: "When true, open in a fresh tab. Recommended for the first navigate of any task.", Example: true},
			{Name: "args.group_title", Description: "Optional visible label for the tab group the new tab joins.", Example: "research"},
			{Name: "session", Description: "Session id; tabs in the same session share lifetime. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "navigate_failed", Description: "Browser refused or timed out loading the URL.", Recovery: "Check the URL is reachable; retry with a longer effective timeout on the channel."},
		},
		Notes: "Returns `{success, url, tabId}`. The returned `tabId` is implicit context for subsequent calls in the same session.",
	},

	TypeFindTab: {
		Description: "Locate an already-open tab by URL/domain. Use when the user references \"my current X\" page.",
		PayloadExample: json.RawMessage(
			`{"args":{"url":"https://www.kimi.com","active":true},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.url", Required: true, Description: "URL or bare domain to match. Substring/domain match.", Example: "https://www.kimi.com"},
			{Name: "args.active", Description: "When true, pick the tab the user is currently viewing. Default: leftmost match.", Example: true},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "tab_not_found", Description: "No open tab matched the URL.", Recovery: "Fall back to kimibridge.navigate to open a fresh tab."},
		},
		Notes: "Returns `{success, url, tabId}`. Prefer this over `navigate` when the user already has the page open — preserves their scroll / form state.",
	},

	TypeSnapshot: {
		Description: "Capture the accessibility tree of the current tab as text. The primary tool for reading page content and locating elements (returns @eNNN refs for click/fill).",
		PayloadExample: json.RawMessage(
			`{"args":{},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "no_active_tab", Description: "Session has no active tab to snapshot.", Recovery: "Open a tab with kimibridge.navigate first."},
		},
		Notes: "Returns `{url, title, tree}`. The tree is plain text — elements appear as `tag \"label\" @e<n>`; pass the `@e<n>` ref to click / fill.",
	},

	TypeClick: {
		Description: "Click an element. Accepts the @eNNN ref from snapshot or a CSS selector.",
		PayloadExample: json.RawMessage(
			`{"args":{"selector":"@e42"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.selector", Required: true, Description: "@eNNN ref (preferred) or CSS selector.", Example: "@e42"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "selector_not_found", Description: "Selector matched no element in the current snapshot.", Recovery: "Re-call snapshot to refresh refs; the DOM may have changed."},
		},
		Notes: "Returns `{success, tag, text}`. Uses synthetic `el.click()` — does not fire mouse-move/hover; for hover effects prefer evaluate.",
	},

	TypeFill: {
		Description: "Type text into an input. Works on <input>, <textarea>, and contenteditable (ProseMirror/Lexical/Slate).",
		PayloadExample: json.RawMessage(
			`{"args":{"selector":"@e7","value":"hello world"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.selector", Required: true, Description: "@eNNN ref or CSS selector.", Example: "@e7"},
			{Name: "args.value", Required: true, Description: "Text to enter. Replaces existing content.", Example: "hello world"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "selector_not_found", Description: "Selector matched no element.", Recovery: "Re-snapshot to refresh refs."},
			{Code: "fill_unsupported", Description: "Target is not a fillable element.", Recovery: "Confirm the element is input/textarea/contenteditable; otherwise use evaluate."},
		},
		Notes: "Returns `{success, tag, mode}` where mode ∈ {value, contenteditable}.",
	},

	TypeEvaluate: {
		Description: "Run arbitrary JavaScript in the page. Supports async/await. Use as escape hatch for things click/fill/snapshot can't express.",
		PayloadExample: json.RawMessage(
			`{"args":{"code":"return document.title"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.code", Required: true, Description: "JS source. Return values are JSON-serialised; non-serialisable returns become `{type:'undefined'}`.", Example: "return document.title"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "evaluate_failed", Description: "JS threw or page rejected the script.", Recovery: "Wrap in try/catch and return the diagnostic explicitly."},
		},
		Notes: "Returns `{type, value}`. Long-running scripts share the daemon's per-call timeout.",
	},

	TypeScreenshot: {
		Description: "Capture the current tab (or a single element) as PNG/JPEG. Daemon writes to disk and returns the path.",
		PayloadExample: json.RawMessage(
			`{"args":{"format":"png","quality":90,"path":"/tmp/page.png"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.format", Description: "\"png\" (default) or \"jpeg\".", Example: "png"},
			{Name: "args.quality", Description: "0-100. Applies to jpeg only.", Example: 90},
			{Name: "args.selector", Description: "Optional @eNNN ref or CSS selector to capture just that element.", Example: "@e12"},
			{Name: "args.path", Description: "Output path. Daemon overwrites if it exists. Omit to use an OS temp file.", Example: "/tmp/page.png"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "screenshot_failed", Description: "Daemon could not capture the tab/element.", Recovery: "Confirm the tab is loaded; try without selector to capture full page."},
		},
		Notes: "Returns `{format, path, sizeBytes, mimeType}`. The agent reads the file via its Read tool; the file is not embedded in the response payload.",
	},

	TypeNetwork: {
		Description: "Inspect the current tab's network activity. Subcommands: start (begin capture), stop, list (recent requests), detail (one request).",
		PayloadExample: json.RawMessage(
			`{"args":{"cmd":"list","filter":"api.kimi.com"},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.cmd", Required: true, Description: "One of: start, stop, list, detail.", Example: "list"},
			{Name: "args.filter", Description: "Substring filter on request URL. Used by list.", Example: "api.kimi.com"},
			{Name: "args.requestId", Description: "Request id (from list output). Required for detail.", Example: "req-123"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "capture_not_started", Description: "list/detail called before start.", Recovery: "Call kimibridge.network with cmd=\"start\" first."},
		},
		Notes: "Use sparingly — captures grow unbounded until stop. Always call cmd=\"stop\" when done.",
	},

	TypeUpload: {
		Description: "Attach files to a file-input element via its selector.",
		PayloadExample: json.RawMessage(
			`{"args":{"selector":"input[type=file]","files":["/tmp/cover.png"]},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.selector", Required: true, Description: "@eNNN ref or CSS selector pointing at the <input type=file>.", Example: "input[type=file]"},
			{Name: "args.files", Required: true, Description: "Absolute file paths on the daemon machine. The extension uploads them to the page.", Example: []string{"/tmp/cover.png"}},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "file_not_found", Description: "One of the files did not exist on the daemon machine.", Recovery: "Verify paths are absolute and readable by the daemon process."},
			{Code: "selector_not_found", Description: "Selector matched no element.", Recovery: "Re-snapshot."},
		},
		Notes: "Returns `{success, fileCount}`.",
	},

	TypeSaveAsPDF: {
		Description: "Render the current page to PDF.",
		PayloadExample: json.RawMessage(
			`{"args":{"paper_format":"A4","landscape":false,"scale":1.0,"print_background":true},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "args.paper_format", Description: "Standard paper name (A4, Letter, ...).", Example: "A4"},
			{Name: "args.landscape", Description: "Orientation toggle.", Example: false},
			{Name: "args.scale", Description: "0.1-2.0 zoom factor.", Example: 1.0},
			{Name: "args.print_background", Description: "Whether to include background colours / images.", Example: true},
			{Name: "args.path", Description: "Output path. Daemon overwrites if it exists. Omit for OS temp file named after page title.", Example: "/tmp/report.pdf"},
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "save_pdf_failed", Description: "Browser refused to render the page to PDF.", Recovery: "Verify the page is fully loaded; try toggling print_background."},
		},
		Notes: "Returns `{path, sizeBytes, mimeType, pageTitle}`. Read the file via the agent's Read tool.",
	},

	TypeListTabs: {
		Description: "List all tabs in the current session.",
		PayloadExample: json.RawMessage(
			`{"args":{},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		Notes: "Returns `{success, tabs: [{tabId, url, title, active, groupTitle}]}`.",
	},

	TypeCloseTab: {
		Description: "Close the current tab in the session.",
		PayloadExample: json.RawMessage(
			`{"args":{},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		Notes: "Returns `{success, closed: bool}`. Closes only the active tab — use close_session to drop the whole session.",
	},

	TypeCloseSession: {
		Description: "Close every tab in the session. Always call at task end.",
		PayloadExample: json.RawMessage(
			`{"args":{},"session":"kimi"}`,
		),
		PayloadFields: []adapter.FieldDoc{
			{Name: "session", Description: "Session id. Default: \"kimi\".", Example: "kimi"},
		},
		Notes: "Returns `{success, closed: int}` where `closed` is the number of tabs released.",
	},
}
