package claudecode

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// init self-registers the claude-code engine as its OWN actor class ("claude")
// into the one actor registry — flat, peer to go-kimi and the tool classes
// (echo/xhs/device). An agent's engine IS its class (kind=agent); there is no
// umbrella "agent" class and no second registry. cmd blank-imports this package
// (via agent/all). The claude SDK stays quarantined here; the registry never
// imports it.
func init() { registry.Register("claude", registry.ClassDecl{Kind: actor.KindAgent, New: NewDecl}) }
