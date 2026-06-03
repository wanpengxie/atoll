// Package store provides sqlite-backed implementations of the
// runtime/storespec channel-local contracts (the kernel-only leaf seam).
//
// CONFINEMENT (Go internal/): this package lives under runtime/internal so the
// compiler restricts it to runtime/... importers. The raw channel-log write
// (messages.Append — the only harness-bypassing INSERT into messages) is thus
// physically unreachable from business code (lib/**, adapters/**) and from
// downstream hosts (server/**, cmd/**). The outside world receives storespec
// INTERFACES (read/append ports) + named control-plane ops, never the concrete
// store. "single writer by construction" is enforced structurally here, not by
// convention. (End-state: the channel-home actor owns the db handle outright —
// then there is no second handle to confine.)
//
// Authoritative spec: runtime-construction-spec §1.3 (storespec ports) +
// L2 §1.4 schema.
//
// Files:
//
//   - sqlite.go            — sql.DB factory (channel-local)
//   - schema.go            — DDL constants (messages / actor_registry). v2: no
//     worker_locks (single-writer server harness, no channel-write lease), no
//     action_ledger (turn-replay idempotency is an application concern, not
//     substrate truth), and no actor_cursors (a per-actor consumption offset is
//     the consumer's own bookkeeping, not substrate truth).
//   - messages.go          — storespec.MessageLog impl.
//   - actors.go            — storespec.Registry impl.
package store
