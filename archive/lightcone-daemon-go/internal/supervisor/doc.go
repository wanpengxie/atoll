// Package supervisor implements the v4 worker lifecycle layer per
// L2 §1.4.9 (worker_locks) + §1.4.10 (supervisor loop + backlog scan).
//
// Three pieces live here:
//
//   - worker_locks.go: Acquire / Heartbeat / Release CAS primitives on
//     the worker_locks table. fencing_token bumps on every steal so a
//     stolen worker's subsequent harness writes get rejected by
//     `worker_fencing_stale` at write time (the harness check lands in
//     T7).
//
//   - backlog.go: BacklogScan executes the L2 §1.4.10 normative SQL —
//     the cursor-delta plus the five trigger-gateway filters
//     (visibility / self-trigger / audience / not_before / expires_at).
//     Output feeds straight into a fresh worker's SpawnContext.Backlog.
//
//   - loop.go: Loop.Run drives one (channel, agent) pair. Wake-up
//     sources are the 10s ticker (default Period) AND the per-spawn
//     OS-exit hook channel, so a worker crash fires respawn within
//     milliseconds. Tick is the single iteration entrypoint exposed to
//     tests; Run is the production driver loop.
//
// Out of scope (left to other M1.3 tickets):
//
//   - The real go-kimi worker binary — T10 plugs in an exec.Cmd-based
//     Spawner; this ticket ships only the Spawner / Worker interfaces
//     and uses fake implementations in tests.
//   - Harness step 1 fencing_token check — T7. The reasons for staying
//     decoupled: harness owns the "write reject" pathway; supervisor
//     only owns the lease/spawn pathway. The bridge is the
//     SpawnContext.FencingToken value the worker carries.
//
// Authoritative spec references: .dalek/pm/v4-layer2-spec.md §1.4.9 and
// §1.4.10 (M1.3 ticket T6 in .dalek/pm/m1.3-tickets.md).
package supervisor
