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
//   - sqlite.go            — sql.DB factory (channel-local + daemon-local)
//   - schema.go            — DDL constants (messages / actor_registry /
//     actor_cursors / type_registry / action_ledger).
//     v2: no worker_locks table — the worker lease is volatile, in compute
//     memory (runtime/workerhost), not a sqlite row.
//   - messages.go          — storespec.MessageLog impl.
//   - cursors.go           — storespec.Cursors impl.
//   - actors.go            — storespec.Registry impl.
//   - ledger.go            — storespec.Ledger impl.
package store
