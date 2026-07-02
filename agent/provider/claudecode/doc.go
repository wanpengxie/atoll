// Package claudecode is the SECOND looper engine: the Claude
// Code agent, bound to the one actor face (lib/behavior + metatool.Shell) exactly
// like the go-kimi looper. It proves the looper integration contract is real — the
// shared mechanical skeleton (Receive / turn-queue / shell / harness.Pen /
// author#2) is engine-agnostic; only three things differ from the go-kimi bridge:
//
//   - engine: go-claude-agent-sdk's ClaudeClient (which shells out to the
//     `claude` CLI), vs go-kimi's in-process Agent.
//   - metatool injection: an in-process MCP server bridging the 7 meta-tools
//     (call_actor …) into the claude tool surface, vs go-kimi's AdditionalTools.
//   - resume: a claude session id (WithResume seed in / ResultMessage.SessionID
//     out → the durable state slot), vs go-kimi's WorkDir last-session.
//
// Like the go-kimi bridge: Receive (cell goroutine) never blocks; a private loop
// (the client edge — blocking legal) runs claude turns serially; responses flow
// through metatool.Shell.Deliver. The agent core (top-level agent/) dispatches to
// THIS engine when agents.looper = claude; this package self-registers via init().
package claudecode
