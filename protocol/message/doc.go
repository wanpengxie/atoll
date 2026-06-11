// Package message defines the v4 message envelope, message kind /
// visibility enums, the core-type table, and the terminal-failure reason
// closed set.
//
// Authoritative spec:
//
//   - Envelope fields           — L0 §2.1 (.dalek/pm/proto-layer0.md)
//   - Kind closed set           — L0 §3.1 invariant I7 + proto-layer0 §3
//   - Visibility enum           — L0 §2.4
//   - Terminal-failure reason   — proto-layer0 §2.6 (INVARIANT-10)
//
// The package depends on kernel/actor for L0 sender identity primitives.
// Pure proto: no context, no storage, no engine interfaces. Harness reject
// reasons + install reasons are the write/install ENGINES' errno vocabulary
// and live with those engines outside this package; reason→HTTP mapping is
// a binding concern, also outside this package.
package message
