// Package message holds the v4 envelope, kind/visibility/reason enums,
// and the canonical_hash algorithm (RFC 8785 + SHA-256).
//
// kernel/message is the protocol type mirror of L0 §2 / §3. The full
// migration from pkg/v4types lands in T1; this file is the T2
// skeleton.
package message

// Kind is the v4 message ADT classifier (event / request / response).
// Once written, kind is immutable (L0 §2.2).
//
// Authoritative spec: L0 §2.1 envelope `kind` field + v4-message-
// definition §3. Closed set per L0 §3.1 invariant I7.
//
// TODO(T1): expand with helpers + tests; pkg/v4types.Kind will become a
// type alias.
type Kind string

// Kind enum — closed set per L0 §3.1.
const (
	KindEvent    Kind = "event"
	KindRequest  Kind = "request"
	KindResponse Kind = "response"
)

// Visibility is the envelope `visibility` field — 3-value closed set
// (L0 §2.4). Once written, visibility is immutable.
type Visibility string

// Visibility enum — closed set per L0 §2.4.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)
