// Package all blank-imports every in-tree agent engine class so ONE binary
// packages the agent stack. Each engine package's init() registers itself as a
// flat actor class (kind=agent) into the one registry — claude / go-kimi are
// PEERS of the tool classes (echo/xhs/device), NOT variants of an umbrella
// "agent" class (there is none).
//
// Symmetric to actors/all (which packages the tool actors). Kept separate on
// purpose: the LLM engine SDKs are quarantined under agent/provider/*. cmd wires
// both aggregates. Adding an engine = a new agent/provider/<x> package with an
// init() + one blank-import line here.
package all

import (
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/claudecode" // engine class: claude
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/kimi"       // engine class: go-kimi
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/script"     // engine class: script (deterministic, e2e loop)
)
