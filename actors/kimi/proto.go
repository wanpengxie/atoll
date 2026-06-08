package kimi

import "github.com/wanpengxie/ActOS/protocol/actor"

const (
	ActorName             = "kimi"
	DefaultAdapterActorID = actor.ActorID("tool:kimi")
	DefaultMaxPendingMs   = int64(30_000)

	Binding = actor.BindingRuntimeInboundViaRelay

	TypeCommand = "kimi.command"
)

var AllTypes = []string{TypeCommand}

var ToolNames = map[string]struct{}{
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

func IsToolName(name string) bool {
	_, ok := ToolNames[name]
	return ok
}
