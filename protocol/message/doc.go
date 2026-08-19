// Package message defines the message envelope, its closed transport
// vocabularies, the reserved system type table, and response failure detail.
//
// Key invariants:
//
//   - Kind is a closed set.
//   - Visibility is the closed attention set {public, system}.
//   - Terminal-failure reason is a closed set.
//
// The package depends on protocol/actor and protocol/channel for identities.
// Pure proto: no context, no storage, no engine interfaces. Harness reject
// reasons + install reasons are the write/install ENGINES' errno vocabulary
// and live with those engines outside this package; reason→HTTP mapping is
// a binding concern, also outside this package.
package message
