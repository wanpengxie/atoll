//go:build !atolltestfast

package store

// syncPragma is the sqlite synchronous level every channel DB opens with.
// Production is always NORMAL (WAL-safe durability). The atolltestfast build
// tag — injected ONLY by the Makefile test targets, structurally impossible
// to carry into a production binary — swaps it to OFF (pragma_sync_fast.go):
// fsync protects data across a process CRASH, which a test run never
// recovers from, so for tests it is pure wasted disk latency (the dominant
// physical cost of every store/e2e test; test 提速③).
const syncPragma = "NORMAL"
