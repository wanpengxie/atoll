// Package store provides sqlite-backed implementations of the
// runtime/storespec channel-local contracts (the kernel-only leaf seam).
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
