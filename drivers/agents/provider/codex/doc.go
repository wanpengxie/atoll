// Package codex translates one generation-scoped `codex app-server` process
// into the provider-neutral driver protocol. Lifecycle arbitration, retries,
// watchdogs, canonical turns, and user terminals belong to the shared runtime
// and Base rather than this provider boundary.
package codex
