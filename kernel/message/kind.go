// Package message defines the v4 message envelope, message kind /
// visibility enums, the core-type table, the terminal-failure reason
// closed set, and the RFC 8785 canonical hash function used by the
// Message-Write Harness.
//
// Authoritative spec:
//
//   - Envelope fields           — L0 §2.1 (.dalek/pm/proto-layer0.md)
//   - Kind closed set           — L0 §3.1 invariant I7 + proto-layer0 §3
//   - Visibility enum           — L0 §2.4
//   - Terminal-failure reason   — proto-layer0 §2.6 (INVARIANT-10)
//   - Canonical hash            — L2 §1.4.10.2
//
// The package depends on kernel/actor for L0 sender identity primitives.
// Pure proto: no context, no storage, no engine interfaces. Harness reject
// reasons + install reasons are the write/install ENGINES' errno vocabulary
// and live with those engines in runtime (not here); reason→HTTP mapping is
// a binding concern and lives in server/gateway.
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

// String returns the wire form.
func (k Kind) String() string { return string(k) }

// Visibility is the envelope `visibility` field — 3-value closed set
// covering who in the channel can query-see this message.
//
// Authoritative spec: proto-layer0 §2.4 (round-3 cluster F). Once
// written, visibility is immutable.
//
// Semantics:
//   - public  — visible to every channel member.
//   - private — only sender + actors in audience may see this message.
//   - system  — protocol-internal metadata / intermediate output
//     (e.g. agent.text progress bubbles, placement notices, bootstrap
//     events). View fanout suppresses these from the default UI view;
//     they still persist in the channel message log (audit trail).
type Visibility string

// Visibility enum — closed set per proto-layer0 §2.4.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

// AllVisibilities enumerates every valid Visibility value, in spec order.
var AllVisibilities = []Visibility{VisibilityPublic, VisibilityPrivate, VisibilitySystem}

// String returns the wire form.
func (v Visibility) String() string { return string(v) }
