package claudecode

import "github.com/wanpengxie/ActOS/agent"

// init self-registers the claude-code engine under its canonical looper key
// ("claude" — the value agents.looper carries) into the agent subsystem's
// looper-registry. cmd blank-imports this package (via agent/all). The claude
// SDK stays quarantined here; the agent core never imports it.
func init() { agent.RegisterLooper("claude", NewDecl) }
