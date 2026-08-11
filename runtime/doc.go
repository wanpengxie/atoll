// Package runtime is the reusable execution substrate for atoll — the v2
// BEAM-style engine layer. It imports only protocol.
//
// runtime/ owns:
//
//   - storespec        — kernel-only leaf contracts (store ports, message
//     plane).
//   - resourcespec     — kernel-only leaf contract for the plane-2 R +
//     driver seam (object plane).
//   - timerspec        — kernel-only leaf contract for the durable
//     pending-timer store (time axis).
//   - internal/store   — sqlite implementations of ALL three contract
//     leaves; the raw *sql.DB is confined here (Go internal/), exposed only
//     as interfaces via OpenChannel → ChannelStores.
//   - harness          — the 9-step Message-Write chain; the single writer of
//     channel truth (opaque Pen / Minter seam).
//   - accessdoor       — the plane-2 access door (caller-welded AccessHandle
//     / AccessMinter over the resourcespec contracts).
//   - schedule         — the time-axis engine (author-welded ScheduleHandle /
//     Minter over the timerspec contract; two lifecycle families).
//   - actorrt          — actor hosts by transport distance: cell (in-proc,
//     mailbox=chan) + port (out-of-proc, mailbox=connection). Death is an
//     obs down-edge published to watchers; closure (materialising
//     receiver_unavailable) lives DOWNSTREAM of that edge, not here.
//   - ipc              — the port wire protocol (length-prefixed frames over
//     a connection: handshake / handshake_ack / deliver / emit / emit_ack /
//     down / cancel / obs).
//
// Boundary rules and their enforcement:
//
//   - runtime/* MAY import protocol/* and runtime/* (self).
//   - protocol/* MUST NOT import runtime/* — mechanically enforced by
//     archtest.TestProtocolPurityAndDirection (direction + purity locks).
//   - This root package is PURE ASSEMBLY (storeopen/scheduleopen) and may
//     only be imported by platform — archtest.TestRuntimeAssemblyConfinedToPlatform.
//
// TODO(lint-rebuild): the one residual unmechanised edge is runtime/* → lib//
// composition-root back-flow for lib leaf packages that do not themselves depend on
// runtime (the major reverse edges are already impossible via Go import
// cycles). Fold into the deferred v2-topology lint rebuild.
package runtime
