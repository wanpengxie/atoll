// Package storespec is the kernel-only leaf that declares the channel
// store's stateful seam contracts — the interfaces runtime/store implements
// over sqlite and the engine consumers (harness / trigger / typeinstall /
// workerhost / server) depend on.
//
// Why a dedicated leaf (runtime-construction-spec §1.3 / topology §8.3):
// these contracts are MULTI-consumer (harness writes the log + reads the
// type/actor registries; trigger reads the registry to resolve audience;
// workerhost reserves the ledger; server reads projections). Putting them
// in any one consumer (e.g. runtime/harness) would force the other
// consumers to import that consumer just for a contract, scrambling the
// dependency graph. A kernel-only leaf breaks the store ↔ harness cycle
// (store implements storespec; harness consumes storespec; neither imports
// the other) — the Go-idiomatic ports pattern.
//
// storespec imports ONLY kernel (pure types). It declares NO fencing
// (v2 channel single-writer = server harness by construction; channel-write
// fence is obsolete — runtime-construction-spec §4.1). AppendError.Reason is
// a plain string (the wire form of a harness reject reason) so storespec does
// not import runtime/harness for the typed constant — that would re-introduce
// the cycle (storespec → harness → storespec). The harness chain maps the
// string back to its typed HarnessRejectReason at the boundary.
package storespec
