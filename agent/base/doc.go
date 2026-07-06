// Package base is the agent Kind's第0件 skeleton (期10 S1): the fixed internals
// EVERY agent shares, sitting on the lib/actorbase Proc底座 so an agent is a
// standard actor whose only extra是四件固定内脏 — 回合弧, 工具面, 输出形,
// 会话记忆 (spec §1). These four are the SKELETON (identical across providers);
// how an engine actually speaks (message shape, tool injection) is the
// ADAPTATION — captured behind the Engine contract's three件套 (Turn / tool
// access / output mapping). A provider (agent/provider/*) writes ONLY an Engine
// implementation; the loop, mailbox, response分拣, describe自答, checkpoint挂账
// are all白拿 from here.
//
// SUBSTRATE本质定位 (not "what agents need"): the actorbase engine already ships
// the workQ + worker + call ledger当年两家手搓 turnQ/runLoop 想要的东西, so the
// base is a bare actorbase.Proc — zero self-built queue/loop. Responses to the
// agent's own outbound calls are matched by the engine's call ledger (never
// surface in Recv), so this Proc's Recv only ever handles a trigger (request /
// event) — describe机械答 or a turn.
//
// ENGINE-AGNOSTIC (archtest TestEngineQuarantine): this package NEVER imports an
// LLM engine SDK — those are quarantined to agent/provider/*. The base speaks
// only the substrate vocabulary (lib/actorbase.Sys) plus the meta-tool catalog
// (lib/metatool) and the describe contract (lib/introspect).
package base
