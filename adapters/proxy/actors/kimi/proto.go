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
	DefaultBaseURL        = "http://127.0.0.1:10086"
	DefaultMaxPendingMs   = int64(30_000)

	TypeCommand = "kimi.command"
)

var AllTypes = []string{TypeCommand}

const actorDescription = "Drive the user's local Kimi WebBridge daemon through the proxy daemon. Use it for browser navigation, screenshots, snapshots, clicks, fills, and JavaScript evaluation on the user's real browser session."

const actorSkillDoc = "" +
	"# kimi\n" +
	"\n" +
	"`kimi.command` forwards one command to the local Kimi WebBridge daemon at `http://127.0.0.1:10086/command`.\n" +
	"\n" +
	"Payload shape:\n" +
	"\n" +
	"```json\n" +
	"{\"action\":\"navigate\",\"args\":{\"url\":\"https://example.com\",\"newTab\":true},\"session\":\"kimi\"}\n" +
	"```\n" +
	"\n" +
	"Common actions mirror kimi-webbridge: `navigate`, `find_tab`, `snapshot`, `click`, `fill`, `evaluate`, `screenshot`, `network`, `upload`, `save_as_pdf`, `list_tabs`, `close_tab`, `close_session`.\n"

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
			Description:        "Forward a single Kimi WebBridge command to the user's local daemon.",
			PayloadExample: json.RawMessage(
				`{"action":"snapshot","args":{},"session":"kimi"}`,
			),
			PayloadFields: []adapter.FieldDoc{
				{Name: "action", Required: true, Description: "Kimi WebBridge command name.", Example: "snapshot"},
				{Name: "args", Description: "Command-specific JSON object.", Example: map[string]any{"url": "https://example.com"}},
				{Name: "session", Description: "Browser session id. Default is the daemon's own default.", Example: "kimi"},
			},
			ErrorCodes: []adapter.ErrorDoc{
				{Code: "daemon_unreachable", Description: "The local daemon at 127.0.0.1:10086 could not be reached.", Recovery: "Start kimi-webbridge locally, then wait for actor.readiness.changed."},
				{Code: "daemon_call_failed", Description: "The daemon returned a transport or HTTP error.", Recovery: "Check the local daemon logs and retry after it reports ready."},
				{Code: "tool_failed", Description: "The daemon ran the command but reported a command-level failure.", Recovery: "Inspect detail; refresh snapshot or choose a valid browser target before retrying."},
				{Code: "payload_decode_failed", Description: "The payload was not a JSON object.", Recovery: "Call describe_type for kimi.command and retry with action/args/session."},
			},
			Notes: "Successful responses surface the daemon's `data` object verbatim and add `status:\"completed\"`.",
		},
	}
}
