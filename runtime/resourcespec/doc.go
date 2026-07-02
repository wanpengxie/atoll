// Package resourcespec is the kernel-only leaf that declares the access
// plane's driver + R (authorization relation) contracts — the interfaces
// runtime/internal/store implements over sqlite, dual to storespec on the
// message plane.
//
// Why a dedicated leaf (mirrors storespec): these contracts sit between the
// door (runtime/accessdoor, the consumer) and the store (the implementation).
// A kernel-only leaf keeps the contract independent of both — the door imports
// the seam, never the implementation. The set is closed-but-additive and
// substrate-owned: the leaf is importable ONLY within the runtime tree, so the
// implementation set cannot be extended by downstream code (enforced at the
// compile layer, not by convention).
//
// resourcespec imports ONLY kernel (pure types) + context/errors. It declares
// the object-lifecycle truth (R + existence) as Registry and the byte realizer
// as Driver, split the way storespec splits Registry from MessageLog:
// R/existence is registry-authoritative, bytes are driver-authoritative. The
// one event crossing both — op=create — is atomic and lives entirely on
// Registry.Create, never orchestrated as two door-visible steps.
package resourcespec
