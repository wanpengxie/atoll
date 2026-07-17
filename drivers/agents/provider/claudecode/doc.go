// Package claudecode is the SECOND agent engine: the Claude Code agent, built
// on agent/base's skeleton (期10 S5) exactly like the go-kimi provider. It
// implements base.Engine (the model-adaptation三件套 — Turn / Describe /
// Checkpoint / Close); the mailbox loop, turn queue, response分拣, describe
// dispatch, per-turn checkpoint挂账, and emit all live in agent/base. Only two
// things differ from the go-kimi engine:
//
//   - engine: go-claude-agent-sdk's ClaudeClient (which shells out to the
//     `claude` CLI), vs go-kimi's in-process Agent.
//   - metatool injection: an in-process MCP server bridging the neutral
//     base.BuildMCPCatalog (call_actor …) into the claude tool surface, vs
//     go-kimi's AdditionalTools.
//
// Durable resume (10.0) is on: the claude session id from each ResultMessage is
// checkpointed on sys.State (agent/base) and replayed via WithResume on the next
// incarnation.
//
// The claude SDK stays quarantined here; the registry never imports it. The
// agent core dispatches to THIS engine when the actor declaration class is
// "claude"; this package self-registers via init().
package claudecode
