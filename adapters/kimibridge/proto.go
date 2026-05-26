// Package kimibridge contains the deprecated Kimi WebBridge schema that
// used to be hosted directly by the coagent daemon.
//
// Deprecated: the daemon-side runtime_outbound adapter was removed. New
// Kimi WebBridge calls must go through the proxy daemon actor in
// adapters/proxy/actors/kimi.
package kimibridge

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// AdapterName is the deprecated framework module name kept for schema
// compatibility.
const AdapterName = "kimibridge"

// DefaultAdapterActorID is the deprecated direct daemon actor id.
const DefaultAdapterActorID actor.ActorID = "tool:kimi-webbridge"

// Binding is the deprecated direct adapter binding kept for legacy
// schema references. It is no longer installed by cmd/daemon.
const Binding = actor.BindingRuntimeOutbound

// DefaultBaseURL is the legacy local daemon endpoint.
const DefaultBaseURL = "http://127.0.0.1:10086"

// DefaultMaxPendingMs is the legacy per-request timeout retained in
// type declarations.
const DefaultMaxPendingMs int64 = 30_000

// Type names — closed set per SKILL.md §Tools. Names use the
// `kimibridge.<action>` convention (impl-vocabulary §3.0 #1: namespace
// = adapter id). Wire `action` field on the daemon side keeps the
// short form (navigate / click / fill / ...).
const (
	TypeNavigate     = "kimibridge.navigate"
	TypeFindTab      = "kimibridge.find_tab"
	TypeSnapshot     = "kimibridge.snapshot"
	TypeClick        = "kimibridge.click"
	TypeFill         = "kimibridge.fill"
	TypeEvaluate     = "kimibridge.evaluate"
	TypeScreenshot   = "kimibridge.screenshot"
	TypeNetwork      = "kimibridge.network"
	TypeUpload       = "kimibridge.upload"
	TypeSaveAsPDF    = "kimibridge.save_as_pdf"
	TypeListTabs     = "kimibridge.list_tabs"
	TypeCloseTab     = "kimibridge.close_tab"
	TypeCloseSession = "kimibridge.close_session"

	TypeDaemonOnline  = "kimibridge.daemon.online"
	TypeDaemonOffline = "kimibridge.daemon.offline"
)

// RequestResponseTypes is the closed set of request/response types this
// adapter exposes. Mirrors SKILL.md §Tools — 13 entries; every tool is
// request/response (no event-only tools today; daemon doesn't push
// unsolicited notifications).
var RequestResponseTypes = []string{
	TypeNavigate,
	TypeFindTab,
	TypeSnapshot,
	TypeClick,
	TypeFill,
	TypeEvaluate,
	TypeScreenshot,
	TypeNetwork,
	TypeUpload,
	TypeSaveAsPDF,
	TypeListTabs,
	TypeCloseTab,
	TypeCloseSession,
}

// EventOnlyTypes is the adapter-specific daemon lifecycle projection.
var EventOnlyTypes = []string{
	TypeDaemonOnline,
	TypeDaemonOffline,
}

// AllTypes is the full closed set Declares() exposes.
var AllTypes = append(append([]string{}, RequestResponseTypes...), EventOnlyTypes...)

// typeToAction maps a coagent envelope.type to the wire `action` field
// the daemon expects on POST /command. The adapter side keeps the
// `kimibridge.` prefix; the wire side keeps the short tool name SKILL.md
// documents so any agent / sdk can target the daemon directly without
// going through coagent.
var typeToAction = map[string]string{
	TypeNavigate:     "navigate",
	TypeFindTab:      "find_tab",
	TypeSnapshot:     "snapshot",
	TypeClick:        "click",
	TypeFill:         "fill",
	TypeEvaluate:     "evaluate",
	TypeScreenshot:   "screenshot",
	TypeNetwork:      "network",
	TypeUpload:       "upload",
	TypeSaveAsPDF:    "save_as_pdf",
	TypeListTabs:     "list_tabs",
	TypeCloseTab:     "close_tab",
	TypeCloseSession: "close_session",
}

// ActionForType returns the wire `action` for the given envelope type.
// Returns ("", false) when the type isn't a kimibridge tool — caller
// should reject as harness_type_unknown (Step 5).
func ActionForType(envelopeType string) (string, bool) {
	a, ok := typeToAction[envelopeType]
	return a, ok
}

// DeclarationTypeDeclarations returns the legacy kernel/adapter
// TypeDeclaration map. Browser tools are request/response; daemon
// lifecycle projections are event-only. The
// framework fails install closed when an adapter opts into strict mode
// (non-nil TypeDeclarations) but leaves a Types entry without a row.
func DeclarationTypeDeclarations() map[string]adapter.TypeDeclaration {
	allowed := []message.Kind{message.KindRequest, message.KindResponse}
	out := make(map[string]adapter.TypeDeclaration, len(AllTypes))
	for _, t := range AllTypes {
		row := typeMeta[t]
		row.AllowedKinds = allowed
		row.TerminalConvention = string(adapter.TerminalPayloadStatus)
		out[t] = row
	}
	ev := adapter.TypeDeclaration{
		AllowedKinds:       []message.Kind{message.KindEvent},
		TerminalConvention: string(adapter.TerminalPayloadStatus),
	}
	for _, t := range EventOnlyTypes {
		out[t] = ev
	}
	return out
}
