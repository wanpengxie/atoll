// Package all blank-imports the agent subsystem + every in-tree looper engine so
// ONE binary packages the agent stack. Importing it triggers: the agent core's
// init() (registers the "agent" class into the registry) and each provider's
// init() (registers its looper key into agent's looper-registry).
//
// Symmetric to actors/all (which packages the non-agent actors). Kept separate
// on purpose: actors/ holds no agent, and nothing in actors/ reaches into agent/.
// cmd wires both aggregates. Adding a looper engine = a new agent/provider/<x>
// package with an init() + one blank-import line here.
package all

import (
	_ "github.com/wanpengxie/ActOS/agent"                     // the "agent" class
	_ "github.com/wanpengxie/ActOS/agent/provider/claudecode" // looper: claude
	_ "github.com/wanpengxie/ActOS/agent/provider/kimi"       // looper: go-kimi
)
