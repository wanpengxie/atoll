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
//     / worker_locks).
//   - messages.go          — kernel/harness.MessageLog impl.
//   - cursors.go           — kernel/log.Cursors impl.
//   - actors.go            — kernel/Registry impl.
//   - ledger.go            — kernel/ledger.Ledger impl.
package store
