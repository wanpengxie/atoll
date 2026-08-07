//go:build atolltestfast

package store

// Test-fast build: synchronous(OFF) — see pragma_sync.go for the rationale
// and the structural guarantee that production never carries this tag.
const syncPragma = "OFF"
