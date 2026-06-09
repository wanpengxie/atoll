package xhs

import (
	"strings"

	"github.com/wanpengxie/ActOS/protocol/actor"
)

// AdapterName is the framework module name; lookup key the daemon uses
// to route Manager.OnExternalCallback to this Module.
const AdapterName = "xhs"

// DefaultAdapterActorID is the canonical actor_registry id exposed by the
// proxy daemon for the xhs actor. Cloud daemon production installs this
// actor through proxy facade, not through a static daemon-side xhs behavior.
//
// Concrete deployments MAY override the value via Config.AdapterActorID
// if they run multiple xhs adapter instances in a channel (none today;
// extension reserved).
const DefaultAdapterActorID actor.ActorID = "tool:xhs"

// Binding is the protocol binding this adapter declares. See
// kernel/actor.BindingRuntimeInboundViaRelay + proto-layer0 §2.8.
const Binding = actor.BindingRuntimeInboundViaRelay

// DefaultMaxPendingMs is the domain-xhs-spec per-request budget
// (300s). XHS browser/extension operations can legitimately take
// longer than the generic framework default.
const DefaultMaxPendingMs int64 = 300 * 1000

// Type names — closed set per domain-xhs-spec §1.
//
// xhs.cookie.sync was retired: in chrome-extension mode the adapter
// never owns cookie state (browser holds it transparently; extension
// fetch automatically carries the right cookies for the xiaohongshu
// domain). The legacy "sync cookie" RPC was a holdover from a CLI-mode
// design where a background process needed cookies pushed to it.
//
// Naming: legacy 4 R/R types keep their original spelling for backward
// compatibility (xhs.publish, xhs.search, xhs.note.fetch, xhs.recent.fetch).
// New types adopt the convention `xhs.<tool_suffix>` where `<tool_suffix>`
// is the chrome extension's tool name with the `xhs_` prefix stripped
// (e.g. xhs_publish_long_content → xhs.publish_long_content). The
// adapter→extension wire `cmd` field is resolved via typeToWireCmd
// (handlers.go) instead of naive TrimPrefix, because legacy names like
// `xhs.note.fetch` don't reduce to the extension cmd `get-note`.
const (
	// Legacy 4 R/R types — wire cmd resolved via typeToWireCmd map.
	TypePublish     = "xhs.publish"
	TypeSearch      = "xhs.search"
	TypeNoteFetch   = "xhs.note.fetch"
	TypeRecentFetch = "xhs.recent.fetch"

	// Event-only types — adapter-emitted observability into the channel.
	// TypeNoteArchived: extension reports a note moved to archive (xhs DOM
	// signal, not adapter lifecycle).
	// TypeDeviceOnline / TypeDeviceOffline: adapter projects its device
	// runtime state into the channel so other actors (UI, agent) can
	// observe device availability without reaching into transport state.
	// These events are NOT collaboration truth — they're an operational
	// projection the adapter chooses to publish; replay-derived state of
	// the channel is unaffected if the events are filtered out.
	TypeNoteArchived  = "xhs.note.archived"
	TypeDeviceOnline  = "xhs.device.online"
	TypeDeviceOffline = "xhs.device.offline"

	// Layer 3 provisional response namespaces — non-terminal interim
	// statuses the adapter may emit via ctx.Provisional while a request
	// is in flight (proto-foundation §1.6.3 + proto-layer0 §2.5).
	//
	// Format: `xhs.<name>` — the namespace MUST match the adapter actor
	// id local-name ("tool:xhs" → "xhs") or harness Step 8 rejects with
	// harness_response_status_namespace_mismatch. We do NOT register
	// these into type_registry (v1 spec leaves Layer 3 namespaces free-
	// form; agents read the string and act).
	//
	// XhsStatusLoginQueued models the case where the extension defers a
	// request because the xiaohongshu session is not logged in and the
	// adapter is waiting on a login flow before forwarding.
	XhsStatusLoginQueued = "xhs.login_queued"

	// New R/R types — wire cmd = type suffix with `_` → `-`.
	TypePublishLongContent = "xhs.publish_long_content"
	TypePublishStatus      = "xhs.publish_status"
	TypeCheckLoginStatus   = "xhs.check_login_status"
	TypeInjectScript       = "xhs.inject_script"
	TypeAnalyzeMyProfile   = "xhs.analyze_my_profile"
	TypeAnalyzeProfile     = "xhs.analyze_profile"
	TypeGetNoteComments    = "xhs.get_note_comments"
	TypeGetNoteAnalytics   = "xhs.get_note_analytics"
	TypeGetCreatorMetrics  = "xhs.get_creator_metrics"
	TypeGetTrendingTopics  = "xhs.get_trending_topics"
)

// RequestResponseTypes is the subset that travels request → response.
// Used by Declares() to populate timer registrations only for types the
// framework actually awaits a reply on.
var RequestResponseTypes = []string{
	TypePublish,
	TypeSearch,
	TypeNoteFetch,
	TypeRecentFetch,
	TypePublishLongContent,
	TypePublishStatus,
	TypeCheckLoginStatus,
	TypeInjectScript,
	TypeAnalyzeMyProfile,
	TypeAnalyzeProfile,
	TypeGetNoteComments,
	TypeGetNoteAnalytics,
	TypeGetCreatorMetrics,
	TypeGetTrendingTopics,
}

// EventOnlyTypes is the subset adapter emits as kind=event only.
var EventOnlyTypes = []string{
	TypeNoteArchived,
	TypeDeviceOnline,
	TypeDeviceOffline,
}

// AllTypes is the full closed set Declares() exposes, including the
// event-only rows so type_registry consistency holds
// ("adapter owns the actor → adapter declares every type that
// references the actor as handler").
var AllTypes = append(append([]string{}, RequestResponseTypes...), EventOnlyTypes...)

// typeToWireCmd maps adapter type → chrome extension's daemon cmd name.
// Legacy 4 types are special-cased; new types follow `_`→`-` conversion
// on the suffix. This replaces the previous naive
// `strings.TrimPrefix(env.Type, "xhs.")` which produced `note.fetch`
// for xhs.note.fetch — a name the extension never registered.
var typeToWireCmd = map[string]string{
	// Legacy — extension's existing 4 cmd handlers.
	TypePublish:     "publish",
	TypeSearch:      "search",
	TypeNoteFetch:   "get-note",
	TypeRecentFetch: "get-my-recent",

	// New 10 — kebab-case to match the extension's existing convention
	// (publish-status, get-my-recent style).
	TypePublishLongContent: "publish-long-content",
	TypePublishStatus:      "publish-status",
	TypeCheckLoginStatus:   "check-login-status",
	TypeInjectScript:       "inject-script",
	TypeAnalyzeMyProfile:   "analyze-my-profile",
	TypeAnalyzeProfile:     "analyze-profile",
	TypeGetNoteComments:    "get-note-comments",
	TypeGetNoteAnalytics:   "get-note-analytics",
	TypeGetCreatorMetrics:  "get-creator-metrics",
	TypeGetTrendingTopics:  "get-trending-topics",
}

// WireCmdFor returns the extension-facing `cmd` value for a given
// adapter type. Returns the bare suffix as a fail-open default when the
// type is unknown — the proxy still emits a Command, and the extension
// will surface `not_implemented` if the cmd is unregistered.
func WireCmdFor(envelopeType string) string {
	if v, ok := typeToWireCmd[envelopeType]; ok {
		return v
	}
	return strings.TrimPrefix(envelopeType, "xhs.")
}

// Command is the outbound wire frame the xhs adapter pushes to the
// Chrome extension. It rides inside the `device_transit.recv` frame's
// payload field (impl-layer2 §5.3.2 outbound — adapter → device).
//
// Field shape stays compatible with the M1.3 legacy extension wire
// (see the archived daemon-go xhs device client) so the extension code keeps
// the extension code unchanged on the inbound command path. The CorrelationID slot carries the request_id from
// device_transit.recv.request_id, which equals envelope.id — the
// extension echoes it back inside Callback.RequestID.
type Command struct {
	Type          string         `json:"type"`           // always "command"
	CorrelationID string         `json:"correlation_id"` // = envelope.id (carried via device_transit.recv.request_id)
	Cmd           string         `json:"cmd"`            // type-suffix (e.g. "publish")
	Params        map[string]any `json:"params"`         // domain payload, with metadata stripped
}

// CommandWireType is the constant value of Command.Type. Exported so
// tests assert it stays "command" and Manager.OnExternalCallback can
// reject unrecognized frame types defensively.
const CommandWireType = "command"

// Callback is the device-side reply. It rides inside the
// `device_transit.send` frame's payload field (impl-layer2 §5.3.1
// inbound — device → adapter). The extension's existing callback body
// carries `correlation_id` (= envelope.id); the adapter recovers the
// in-flight envelope by that id.
type Callback struct {
	CorrelationID string         `json:"correlation_id"`
	DeviceID      string         `json:"device_id,omitempty"`
	Status        string         `json:"status"` // "ok" | "error" (+ legacy synonyms)
	Result        map[string]any `json:"result,omitempty"`
	ErrorObj      map[string]any `json:"error,omitempty"`
}
