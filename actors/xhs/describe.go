package xhs

import (
	"encoding/json"
)

// TypeMeta holds per-type metadata for actor describe responses.
type TypeMeta struct {
	Description    string
	PayloadExample json.RawMessage
	PayloadFields  []FieldDoc
	ErrorCodes     []ErrorDoc
	Notes          string
}

// FieldDoc documents a single payload field.
type FieldDoc struct {
	Name        string
	Required    bool
	Description string
	Example     any
}

// ErrorDoc documents a known error code.
type ErrorDoc struct {
	Code        string
	Description string
	Recovery    string
}

// actorDescription is the one-line actor positioning returned by
// list_actors / describe_actor (actor-cli-pattern §9).
const actorDescription = "Automate the user's logged-in XHS (小红书) account via a Chrome extension. Publish notes, search posts, fetch note details, analyze profiles/notes, query creator metrics."

// actorSkillDoc is the markdown usage guide returned by describe_actor.
// Source: domain-xhs-spec §1.1–§1.6 + extension tool catalog.
const actorSkillDoc = "" +
	"# xhs (小红书)\n" +
	"\n" +
	"Drives the user's logged-in XHS account through a Chrome extension that " +
	"connects back to the daemon as a `runtime_inbound_via_relay` device. " +
	"All cookie / auth state lives in the browser — the adapter has none.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"Four legacy tools (closed result schemas):\n" +
	"\n" +
	"- `xhs.publish` — publish a short note (image + text). Returns `{note_id, url, device_id}`.\n" +
	"- `xhs.search` — search XHS feed for a query. Returns `{results: [...]}`.\n" +
	"- `xhs.note.fetch` — fetch a single note by `note_id`. Returns `{note: {...}}`.\n" +
	"- `xhs.recent.fetch` — list the account's recent notes. Returns `{notes: [...]}`.\n" +
	"\n" +
	"Ten extension-tool pass-throughs (variable result shape):\n" +
	"\n" +
	"- `xhs.publish_long_content` / `xhs.publish_status` — long-form publish + polling.\n" +
	"- `xhs.check_login_status` — verify the browser is logged in.\n" +
	"- `xhs.inject_script` — escape hatch; runs arbitrary JS in the XHS tab.\n" +
	"- `xhs.analyze_my_profile` / `xhs.analyze_profile` — profile portrait of self / another user.\n" +
	"- `xhs.get_note_comments` / `xhs.get_note_analytics` — comment + analytics dumps.\n" +
	"- `xhs.get_creator_metrics` — creator-centre metrics.\n" +
	"- `xhs.get_trending_topics` — trending / hot-search listings.\n" +
	"\n" +
	"Three observability events on this channel:\n" +
	"\n" +
	"- `actor.readiness.changed` — framework emits on every adapter readiness transition (public visibility, recommended subscription point for retry-after-online flows).\n" +
	"- `xhs.device.online` / `xhs.device.offline` — adapter-specific device-state projections (system visibility, used by daemon-side observability tooling; not directly visible to SDK subscribers).\n" +
	"- `xhs.note.archived` — agent-emitted (NOT adapter) when an agent moves a note to `archive/`.\n" +
	"\n" +
	"## Typical workflow\n" +
	"\n" +
	"1. `xhs.check_login_status` — verify the browser is signed in. If not, surface to the user.\n" +
	"2. Optional `xhs.analyze_my_profile` / `xhs.get_trending_topics` for context.\n" +
	"3. Publish via `xhs.publish` (short) or `xhs.publish_long_content` + poll `xhs.publish_status`.\n" +
	"4. `xhs.recent.fetch` to confirm the note landed.\n" +
	"\n" +
	"## Device state gate\n" +
	"\n" +
	"Every request fails fast with `reason=receiver_unavailable, error_code=device_offline` " +
	"when the extension is disconnected. Subscribe to `actor.readiness.changed` filtered on " +
	"`actor_id=tool:xhs` to know when retries are worthwhile rather than retrying blindly.\n" +
	"\n" +
	"## Common error surface\n" +
	"\n" +
	"- `device_offline` / `device_token_expired` — extension not reachable; re-bind device.\n" +
	"- `publish_timeout` / `<cmd>_failed` — extension ran the tool but xhs.com rejected.\n" +
	"- `payload_decode_failed` — args JSON not valid.\n" +
	"\n" +
	"## Constraints\n" +
	"\n" +
	"- Duplicate-publish guard is the agent's responsibility (the adapter does not dedupe).\n" +
	"- `images` paths in `xhs.publish` MUST be channel-workdir relative; adapter validates existence.\n"

// typeMeta carries the actor-CLI describe_type convention metadata for
// each xhs type. Source of truth: domain-xhs-spec §1.1–§1.6.
var typeMeta = map[string]TypeMeta{
	TypePublish: {
		Description: "Publish a short note (image + text) to XHS. Returns the note id + URL on success.",
		PayloadExample: json.RawMessage(
			`{"title":"我的第一篇笔记","content":"今天试了一家咖啡店…","tags":["咖啡","探店"],"images":["assets/cover.png"]}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "title", Required: true, Description: "Note title; non-empty UTF-8.", Example: "我的第一篇笔记"},
			{Name: "content", Required: true, Description: "Note body; non-empty UTF-8.", Example: "今天试了一家咖啡店…"},
			{Name: "tags", Description: "Optional string array of tag labels.", Example: []string{"咖啡", "探店"}},
			{Name: "images", Description: "Optional workdir-relative paths (typically under `assets/`). Adapter validates existence.", Example: []string{"assets/cover.png"}},
		},
		ErrorCodes: []ErrorDoc{
			{Code: "publish_timeout", Description: "Extension submitted the note but xhs.com did not confirm in time.", Recovery: "Check `xhs.recent.fetch` — the note may have landed despite the timeout."},
			{Code: "device_offline", Description: "Extension is not connected.", Recovery: "Wait for `xhs.device.online` event."},
			{Code: "device_token_expired", Description: "Actor token expired.", Recovery: "Re-bind the device (UI-side flow)."},
		},
		Notes: "Response is allow-listed: only `{status, reason, note_id, url, device_id, retry_after, error_code, detail}` survive the adapter boundary.",
	},

	TypeSearch: {
		Description: "Search the XHS feed for a query string.",
		PayloadExample: json.RawMessage(
			`{"query":"咖啡","limit":10}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "query", Required: true, Description: "Search keyword; non-empty UTF-8.", Example: "咖啡"},
			{Name: "limit", Description: "Optional positive integer; implementation may cap the upper bound.", Example: 10},
		},
		ErrorCodes: []ErrorDoc{
			{Code: "search_failed", Description: "xhs.com returned an error.", Recovery: "Retry; if persistent, narrow the query."},
			{Code: "device_offline", Description: "Extension not connected.", Recovery: "Wait for `xhs.device.online` event."},
		},
		Notes: "Returns `{results: [{note_id, title, url, author, ...}]}` — result element schema is adapter-defined; the adapter allow-list keeps only `results` on the success branch.",
	},

	TypeNoteFetch: {
		Description: "Fetch a single note by its xhs.com note id.",
		PayloadExample: json.RawMessage(
			`{"note_id":"6537abcdef012345"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "note_id", Required: true, Description: "xhs.com-side note identifier.", Example: "6537abcdef012345"},
		},
		ErrorCodes: []ErrorDoc{
			{Code: "note_not_found", Description: "xhs.com returned 404 / deleted.", Recovery: "Confirm note_id; check via xhs.search."},
			{Code: "device_offline", Description: "Extension not connected.", Recovery: "Wait for `xhs.device.online`."},
		},
		Notes: "Returns `{note: {...}}` — note object schema is adapter-defined.",
	},

	TypeRecentFetch: {
		Description: "List the current account's recent notes.",
		PayloadExample: json.RawMessage(
			`{"limit":20}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "limit", Description: "Optional positive integer.", Example: 20},
		},
		ErrorCodes: []ErrorDoc{
			{Code: "device_offline", Description: "Extension not connected.", Recovery: "Wait for `xhs.device.online`."},
		},
		Notes: "Returns `{notes: [...]}`.",
	},

	TypePublishLongContent: {
		Description: "Publish a long-form note via the extension's long-content flow. Returns variable-shape result (pass-through).",
		PayloadExample: json.RawMessage(
			`{"title":"长文标题","content":"…长文正文…","cover_image":"assets/cover.png"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "title", Required: true, Description: "Note title.", Example: "长文标题"},
			{Name: "content", Required: true, Description: "Long-form body.", Example: "…"},
			{Name: "cover_image", Description: "Optional workdir-relative cover image.", Example: "assets/cover.png"},
		},
		Notes: "Result shape is pass-through from the extension; poll via `xhs.publish_status` for terminal state.",
	},

	TypePublishStatus: {
		Description: "Poll the publish state of a previously submitted note.",
		PayloadExample: json.RawMessage(
			`{"note_id":"draft-1234"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "note_id", Required: true, Description: "Identifier returned by the publish call.", Example: "draft-1234"},
		},
		Notes: "Pass-through result.",
	},

	TypeCheckLoginStatus: {
		Description:    "Check whether the browser is currently logged into xhs.com.",
		PayloadExample: json.RawMessage(`{}`),
		Notes:          "Pass-through. Returns extension-defined `{logged_in: bool, ...}`.",
	},

	TypeInjectScript: {
		Description: "Run arbitrary JavaScript inside the active XHS tab. Escape hatch — prefer the structured tools.",
		PayloadExample: json.RawMessage(
			`{"script":"return document.title"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "script", Required: true, Description: "JS source. Return value is JSON-serialised.", Example: "return document.title"},
		},
		Notes: "Pass-through result. Use sparingly; subject to xhs.com anti-bot detection.",
	},

	TypeAnalyzeMyProfile: {
		Description:    "Read a profile portrait (followers, recent posts, engagement summary) for the logged-in account.",
		PayloadExample: json.RawMessage(`{}`),
		Notes:          "Pass-through result.",
	},

	TypeAnalyzeProfile: {
		Description: "Read another user's public profile portrait.",
		PayloadExample: json.RawMessage(
			`{"profile_url":"https://www.xiaohongshu.com/user/profile/xxx"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "profile_url", Required: true, Description: "Public xhs.com profile URL.", Example: "https://www.xiaohongshu.com/user/profile/xxx"},
		},
		Notes: "Pass-through result.",
	},

	TypeGetNoteComments: {
		Description: "Fetch the comment thread of a note.",
		PayloadExample: json.RawMessage(
			`{"note_id":"6537abcdef012345"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "note_id", Required: true, Description: "xhs.com note id.", Example: "6537abcdef012345"},
		},
		Notes: "Pass-through result.",
	},

	TypeGetNoteAnalytics: {
		Description: "Fetch view / like / save / share metrics for one of the logged-in account's notes.",
		PayloadExample: json.RawMessage(
			`{"note_id":"6537abcdef012345"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "note_id", Required: true, Description: "Note id (must belong to the logged-in account).", Example: "6537abcdef012345"},
		},
		Notes: "Pass-through. Requires creator-centre access.",
	},

	TypeGetCreatorMetrics: {
		Description:    "Fetch creator-centre aggregate metrics for the logged-in account.",
		PayloadExample: json.RawMessage(`{}`),
		Notes:          "Pass-through. Requires the account to be enrolled in the creator centre.",
	},

	TypeGetTrendingTopics: {
		Description:    "List current trending / hot-search topics on XHS.",
		PayloadExample: json.RawMessage(`{}`),
		Notes:          "Pass-through.",
	},

	TypeNoteArchived: {
		Description: "Event-only: an agent moved a note's content into the channel workdir's archive path. NOT emitted by the adapter — agents emit this themselves.",
		PayloadExample: json.RawMessage(
			`{"note_id":"6537abcdef012345","archive_path":"archive/6537abcdef012345.md"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "note_id", Required: true, Description: "Archived note's xhs.com id.", Example: "6537abcdef012345"},
			{Name: "archive_path", Required: true, Description: "Workdir-relative path the content lives at.", Example: "archive/6537abcdef012345.md"},
			{Name: "archived_at", Description: "Optional; if present MUST equal envelope.ts.", Example: 1716643200},
		},
		Notes: "`sender.kind` MUST be `agent` (not tool). The adapter owns this type only for type_registry consistency.",
	},

	TypeDeviceOnline: {
		Description: "Event-only: adapter projects its device-state transition into the channel when the extension comes online.",
		PayloadExample: json.RawMessage(
			`{"device_state":"online","previous_state":"offline","lifecycle_event":"connected","device_id":"dev-abc"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "device_state", Required: true, Description: "Closed set: online | offline | token_expired.", Example: "online"},
			{Name: "previous_state", Description: "Prior state for state-machine clarity.", Example: "offline"},
			{Name: "lifecycle_event", Description: "Closed set: connected | disconnected | token_expired.", Example: "connected"},
			{Name: "device_id", Description: "Optional device identifier captured at actor-token issue time.", Example: "dev-abc"},
		},
		Notes: "Observability projection; visibility=system. Subscribe instead of polling lifecycle state.",
	},

	TypeDeviceOffline: {
		Description: "Event-only: adapter projects its device-state transition into the channel when the extension disconnects or its actor token expires.",
		PayloadExample: json.RawMessage(
			`{"device_state":"offline","previous_state":"online","lifecycle_event":"disconnected"}`,
		),
		PayloadFields: []FieldDoc{
			{Name: "device_state", Required: true, Description: "Closed set: online | offline | token_expired.", Example: "offline"},
			{Name: "previous_state", Description: "Prior state.", Example: "online"},
			{Name: "lifecycle_event", Description: "Closed set: connected | disconnected | token_expired.", Example: "disconnected"},
			{Name: "detail", Description: "Optional human-readable diagnostic.", Example: "websocket closed by peer"},
		},
		Notes: "Observability projection; visibility=system.",
	},
}
