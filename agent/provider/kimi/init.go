package kimi

import "github.com/wanpengxie/ActOS/agent"

// init self-registers the go-kimi engine under its canonical looper key into the
// agent subsystem's looper-registry (agent-spec §10.2 driver-registration). cmd
// blank-imports this package (via agent/all) to trigger it. The edge points
// provider → agent only; the agent core never imports back, so the engine
// (go-kimi) stays quarantined in this package.
func init() { agent.RegisterLooper("go-kimi", NewDecl) }
