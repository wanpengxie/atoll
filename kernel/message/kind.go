// Package message defines the v4 message envelope, message kind /
// visibility enums, the three closed reason sets (harness reject /
// install / terminal failure), and the RFC 8785 canonical hash function
// used by the Message-Write Harness.
//
// Authoritative spec:
//
//   - Envelope fields           — L0 §2.1 (.dalek/pm/v4-layer0-spec.md)
//   - Kind closed set           — L0 §3.1 invariant I7 + v4-message-definition §3
//   - Visibility enum           — L0 §2.4
//   - Reason closed sets        — L1 §10.3
//   - Canonical hash            — L2 §1.4.10.2
//
// The package depends on kernel/actor for L0 sender identity primitives.
package message

// Kind is the v4 message ADT classifier (event / request / response).
//
// Once a message is written, kind is immutable (L0 §2.2 — "kind 写入定死").
type Kind string

// Kind enum — closed set per L0 §3.1 invariant I7.
const (
	KindEvent    Kind = "event"
	KindRequest  Kind = "request"
	KindResponse Kind = "response"
)

// AllKinds enumerates every valid Kind value, in spec order.
var AllKinds = []Kind{KindEvent, KindRequest, KindResponse}

// Visibility is the envelope `visibility` field — 3-value closed set
// covering who in the channel can query-see this message.
//
// Authoritative spec: L0 §2.4. Once written, visibility is immutable.
type Visibility string

// Visibility enum — closed set per L0 §2.4.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

// AllVisibilities enumerates every valid Visibility value, in spec order.
var AllVisibilities = []Visibility{VisibilityPublic, VisibilityPrivate, VisibilitySystem}
