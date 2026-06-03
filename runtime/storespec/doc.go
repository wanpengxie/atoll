// Package storespec is the kernel-only leaf that declares the channel
// store's stateful seam contracts — the interfaces runtime/internal/store
// implements over sqlite.
//
// Why a dedicated leaf (runtime-construction-spec §1.3 / topology §8.3):
// these contracts are MULTI-consumer. Putting the contract inside any one
// consumer would force every other consumer to import that package just for
// the contract, scrambling the dependency graph. A kernel-only leaf keeps the
// contract independent of both the implementation (store) and the consumers —
// the Go-idiomatic ports pattern: a consumer imports the seam, never the
// implementation.
//
// storespec imports ONLY kernel (pure types). It declares NO fencing (the
// channel has a single write path by construction; the channel-write fence is
// obsolete — runtime-construction-spec §4.1). AppendError.Reason is a plain
// string diagnostic code defined by the store implementation, so storespec
// imports nothing beyond kernel; the consumer maps the string into its own
// error domain at the boundary.
package storespec
