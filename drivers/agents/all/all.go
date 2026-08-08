// Package all blank-imports every in-tree agent provider so one binary
// packages the agent stack. Each provider package's init registers itself as a
// flat actor class (kind=agent) into the one registry — codex / script are
// PEERS of the tool classes (echo/xhs/device), NOT variants of an umbrella
// "agent" class (there is none).
//
// Symmetric to actors/all (which packages the tool actors). Kept separate on
// purpose: native protocols are quarantined under agent/provider/*. Adding a
// provider requires only a Provider/Adapter/Worker and one blank import here.
package all

import (
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/codex"
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/script"
)
