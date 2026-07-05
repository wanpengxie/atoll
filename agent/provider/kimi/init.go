package kimi

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// init self-registers the go-kimi engine as its OWN actor class ("go-kimi") into
// the one actor registry — flat, peer to claude and the tool classes. An agent's
// engine IS its class (kind=agent); no umbrella "agent" class, no second
// registry. cmd blank-imports this package (via agent/all). The go-kimi SDK stays
// quarantined here; the registry never imports it.
func init() { registry.Register("go-kimi", registry.ClassDecl{Kind: actor.KindAgent, New: NewDecl}) }
