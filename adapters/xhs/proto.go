package xhs

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// AdapterName is the framework Module identifier (Manager.OnExternalCallback
// route key).
const AdapterName = "xhs-scaffold"

// DefaultAdapterActorID is the canonical tool actor row this scaffold
// binds — same id the T3 device adapter will own once the runtime_inbound_via_relay
// path lands. domain-xhs-spec §1 keeps adapter-emitted responses under
// sender.id=tool:xhs-adapter.
const DefaultAdapterActorID actor.ActorID = "tool:xhs-adapter"

// Binding is the T2 closed-enum value — embedded so the daemon
// composition root can install it without DeviceTransit. T3 will swap
// this to BindingRuntimeInboundViaRelay when the device adapter goes live.
const Binding = actor.BindingEmbedded

// DefaultMaxPendingMs is the sane per-request default. Long-running
// xhs operations must opt in with an explicit override instead of
// inheriting a broad framework default.
const DefaultMaxPendingMs int64 = 30 * 1000

// Type names — same closed set the device adapter uses (domain-xhs-spec §1).
const (
	TypePublish      = "xhs.publish"
	TypeSearch       = "xhs.search"
	TypeNoteFetch    = "xhs.note.fetch"
	TypeRecentFetch  = "xhs.recent.fetch"
	TypeCookieSync   = "xhs.cookie.sync"
	TypeNoteArchived = "xhs.note.archived" // event-only
)

// RequestResponseTypes is the subset that travels request → response.
var RequestResponseTypes = []string{
	TypePublish,
	TypeSearch,
	TypeNoteFetch,
	TypeRecentFetch,
	TypeCookieSync,
}

// AllTypes is the full closed set the scaffold declares. TypeNoteArchived
// is event-only — the harness step 5 allowed_kinds rule excludes
// request/response for it.
var AllTypes = append(append([]string{}, RequestResponseTypes...), TypeNoteArchived)
