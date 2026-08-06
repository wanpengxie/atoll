// Package message defines the v4 message envelope, message kind /
// visibility enums, and the terminal-failure reason closed set.
//
// Key invariants:
//
//   - Kind is a closed set.
//   - Visibility is the closed attention set {public, system}.
//   - Terminal-failure reason is a closed set.
//
// The package depends on protocol/actor for sender identity primitives.
// Pure proto: no context, no storage, no engine interfaces. Harness reject
// reasons + install reasons are the write/install ENGINES' errno vocabulary
// and live with those engines outside this package; reason→HTTP mapping is
// a binding concern, also outside this package.
package message
