package kimi

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

const (
	AdapterName           = "kimi"
	DefaultAdapterActorID = actor.ActorID("tool:kimi")
	DefaultMaxPendingMs   = int64(30_000)

	TypeCommand = "kimi.command"
)

var AllTypes = []string{TypeCommand}

var kimiToolNames = map[string]struct{}{
	"navigate":      {},
	"find_tab":      {},
	"snapshot":      {},
	"click":         {},
	"fill":          {},
	"evaluate":      {},
	"screenshot":    {},
	"network":       {},
	"upload":        {},
	"save_as_pdf":   {},
	"list_tabs":     {},
	"close_tab":     {},
	"close_session": {},
}

func isKimiToolName(name string) bool {
	_, ok := kimiToolNames[name]
	return ok
}

const actorDescription = "Drive the user's Kimi WebBridge chrome extension through the proxy daemon. Use it for browser navigation, screenshots, snapshots, clicks, fills, and JavaScript evaluation on the user's real browser session."

const actorSkillDoc = "" +
	"# kimi\n" +
	"\n" +
	"`kimi.command` forwards one command to the local Kimi WebBridge chrome extension connected to coagent-proxy.\n" +
	"\n" +
	"Payload shape:\n" +
	"\n" +
	"```json\n" +
	"{\"action\":\"navigate\",\"args\":{\"url\":\"https://example.com\",\"newTab\":true},\"session\":\"kimi\"}\n" +
	"```\n" +
	"\n" +
	"Common actions mirror Kimi WebBridge: `navigate`, `find_tab`, `snapshot`, `click`, `fill`, `evaluate`, `screenshot`, `network`, `upload`, `save_as_pdf`, `list_tabs`, `close_tab`, `close_session`.\n"

func Declaration(actorID actor.ActorID, maxPendingMs int64) adapter.Declaration {
	if actorID == "" {
		actorID = DefaultAdapterActorID
	}
	if maxPendingMs <= 0 {
		maxPendingMs = DefaultMaxPendingMs
	}
	return adapter.Declaration{
		Name:             AdapterName,
		ActorID:          actorID,
		Types:            append([]string(nil), AllTypes...),
		TypeDeclarations: DeclarationTypeDeclarations(),
		Binding:          actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs:     maxPendingMs,
		Description:      actorDescription,
		SkillDoc:         actorSkillDoc,
	}
}

func DeclarationTypeDeclarations() map[string]adapter.TypeDeclaration {
	return map[string]adapter.TypeDeclaration{
		TypeCommand: {
			AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
			TerminalConvention: string(adapter.TerminalPayloadStatus),
			Description:        "Forward a single Kimi WebBridge command to the user's chrome extension.",
			PayloadExample: json.RawMessage(
				`{"action":"snapshot","args":{},"session":"kimi"}`,
			),
			PayloadFields: []adapter.FieldDoc{
				{Name: "action", Required: true, Description: "Kimi WebBridge command name.", Example: "snapshot"},
				{Name: "args", Description: "Command-specific JSON object.", Example: map[string]any{"url": "https://example.com"}},
				{Name: "session", Description: "Browser session id. Passed to the extension as _session when omitted from args.", Example: "kimi"},
			},
			ErrorCodes: []adapter.ErrorDoc{
				{Code: "daemon_unreachable", Description: "The embedded Kimi WebBridge server is not running.", Recovery: "Restart coagent-proxy, then wait for actor.readiness.changed."},
				{Code: "extension_disconnected", Description: "The Kimi WebBridge chrome extension is not connected to coagent-proxy.", Recovery: "Install or reload the extension and wait for actor.readiness.changed."},
				{Code: "daemon_call_failed", Description: "The embedded server could not deliver the command or receive the result.", Recovery: "Check coagent-proxy logs and retry after the extension reports ready."},
				{Code: "tool_failed", Description: "The extension ran the command but reported a command-level failure.", Recovery: "Inspect detail; refresh snapshot or choose a valid browser target before retrying."},
				{Code: "payload_decode_failed", Description: "The payload was not a JSON object.", Recovery: "Call describe_type for kimi.command and retry with action/args/session."},
			},
			Notes: "Successful responses surface the extension's `data` object verbatim and add `status:\"completed\"`.",
		},
	}
}
