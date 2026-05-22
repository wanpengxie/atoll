// Package store provides sqlite-backed implementations of kernel
// channel-local interfaces.
//
// Authoritative spec: launch-ticket notes §T3 + L2 §1.4 schema.
//
// Files:
//
//   - sqlite.go            — sql.DB factory (channel-local + daemon-local)
//   - schema.go            — DDL constants (messages / actor_registry /
//     actor_cursors / type_registry / action_ledger
//     / worker_locks / view_sync_outbox /
//     channel_lock).
//   - messages.go          — kernel/log.MessageLog impl.
//   - cursors.go           — kernel/log.Cursors impl.
//   - actors.go            — kernel/actorreg.Registry impl.
//   - ledger.go            — kernel/ledger.Ledger impl.
//   - view_sync_outbox.go  — outbox CRUD + GC (drives runtime/transit).
//   - channel_lock.go      — channel_lock CRUD (fencing_token /
//     owner_epoch / daemon_id / daemon_epoch).
package store
