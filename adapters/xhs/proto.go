package xhs

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// AdapterName is the framework Module identifier (Manager.OnExternalCallback
// route key).
const AdapterName = "xhs-scaffold"

// DefaultAdapterActorID is the canonical tool actor row this scaffold
// binds — same id the T3 device adapter will own once the via_server_transit
// path lands. v4-message-definition §1.2.5 mandates sender.id =
// tool:xhs-adapter on every adapter-emitted response.
const DefaultAdapterActorID actor.ActorID = "tool:xhs-adapter"

// Binding is the T2 closed-enum value — in_process so the daemon
// composition root can install it without DeviceTransit. T3 will swap
// this to BindingViaServerTransit when the device adapter goes live.
const Binding = actor.BindingInProcess

// DefaultMaxPendingMs mirrors the device adapter baseline (5 min). The
// framework arms an F3 timer of this duration on every request; T2's
// mock path replies synchronously inside Handle so the timer never
// fires in the happy path, but the field still feeds type_registry
// install (acceptance #6: missing max_pending_ms → adapter_timeout_missing).
const DefaultMaxPendingMs int64 = 5 * 60 * 1000

// Type names — same closed set the device adapter uses (L4 §2.1).
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
