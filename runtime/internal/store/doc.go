// Package store provides sqlite-backed implementations of the channel-local
// contract leaves — runtime/storespec (message plane), runtime/resourcespec
// (object plane) and runtime/timerspec (time axis).
//
// CONFINEMENT (Go internal/): this package lives under runtime/internal so the
// Go compiler structurally restricts it to runtime/... importers. The raw
// channel-log write (the only INSERT into messages) is thus reachable only
// inside the runtime boundary — no ad-hoc convention is needed. Anything
// outside receives storespec INTERFACES (read/append ports) + named
// control-plane ops, never the concrete store. "single writer by construction"
// is enforced structurally here, not by convention. (End-state: the
// channel-home actor owns the db handle outright — then there is no second
// handle to confine.)
//
// This package implements the storespec ports and the underlying schema.
//
// Files:
//
//   - sqlite.go            — sql.DB factory (channel-local; per-connection
//     pragmas ride the DSN, pool pinned to 1).
//   - schema.go            — DDL constants (messages / actor_registry /
//     resources / resource_grants / actor_state / timers). v2: no
//     worker_locks (the channel has a single write path by construction — a
//     structural invariant, not a per-row lease), no
//     action_ledger (turn-replay idempotency is an application concern, not
//     substrate truth), and no actor_cursors (a per-actor consumption offset is
//     the consumer's own bookkeeping, not substrate truth).
//   - channel.go           — OpenChannel assembly of every seam over one db.
//   - messages.go          — storespec.MessageLog impl.
//   - request_lookup.go    — storespec.RequestLookup impl.
//   - actors.go            — storespec.Registry + membership control plane.
//   - resources.go         — resourcespec.Registry + KindKV driver impl.
//   - state.go             — resourcespec.StateStore impl (actor-scoped locus).
//   - timers.go            — timerspec.TimerStore impl.
package store
